/*
 * Copyright (c) 2022 LETSES.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.letses.platform

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.util.JsonFormat
import kotlinx.collections.immutable.ImmutableMap
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.letses.command.CommandEnvelope
import org.letses.entity.EntityStateMachine
import org.letses.messaging.*
import org.letses.persistence.EventStore
import org.letses.persistence.SagaRepository
import org.letses.persistence.publisher.EventPublisher
import org.letses.saga.SagaDef
import org.letses.saga.SagaEvent
import org.letses.saga.SagaMessageHandlerImpl
import org.letses.saga.Trigger
import org.letses.utils.jackson.registerImmutableCollectionsSerde
import org.letses.utils.launchWithDefer
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

internal class PulsarProtobufSagaRuntime<S : EntityStateMachine<S, T>, T : Trigger>(
    private val sagaDef: SagaDef<S, T>,
    eventStore: EventStore<SagaEvent<T>>,
    eventPublisher: EventPublisher<SagaEvent<T>>,
    private val schemas: ImmutableMap<String, PulsarProtobufSchemas>,
    private val boundedContextName: String,
    private val pulsarClient: PulsarClient,
    private val channelMapper: ChannelMapper
) : SagaRuntime<S, T>, CoroutineScope {

    companion object {
        private val log = LoggerFactory.getLogger(PulsarProtobufSagaRuntime::class.java)
        internal const val LENGTH_FIELD_LENGTH = 8
        internal val headersObjectMapper = ObjectMapper().apply {
            registerKotlinModule()
            registerImmutableCollectionsSerde()
        }
    }

    override val coroutineContext: CoroutineContext = Job()

    private val repo = SagaRepository(sagaDef, eventStore, eventPublisher)

    private val cmdSerializer = { _: String, cmd: CommandEnvelope ->
        val (h, p) = PulsarProtobufCommandProtocol.encode(cmd)
        val headerJson = headersObjectMapper.writeValueAsString(h)
        val headerLength = headerJson.length.toString().padStart(LENGTH_FIELD_LENGTH, '0')
        assert(headerLength.length == LENGTH_FIELD_LENGTH)
        buildString {
            append(headerLength)
            append(headerJson)
            append(JsonFormat.printer().print(p))
        }
    }

    private val handler = SagaMessageHandlerImpl(repo, sagaDef, cmdSerializer)

    override fun start() {
        log.info("Starting saga runtime for ${sagaDef.type.name}")
        sagaDef.subscribedChannels.forEach { fullTopic ->
            val schema = getSchema(fullTopic)
            val subName = "$boundedContextName/${sagaDef.type.name}"
            val finalTopic = channelMapper(fullTopic)
            launchWithDefer {
                val consumer = pulsarClient.newConsumer(schema)
                    .topic(finalTopic)
                    .subscriptionName("$boundedContextName/${sagaDef.type.name}")
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscribeAsync()
                    .await()
                log.info("Pulsar consumer started, topic=$finalTopic, subscriptionName=$subName")

                defer {
                    consumer.closeAsync().await()
                    log.info("Pulsar consumer stopped, topic=$finalTopic, subscriptionName=$subName")
                }

                while (isActive) {
                    val pMsg = consumer.receiveAsync().await()
                    val msg = convertMsg(PayloadAndHeaders(pMsg.properties, pMsg.value)) ?: continue
                    handler.handle(msg)
                    consumer.acknowledgeAsync(pMsg).await()
                }
            }
        }
    }

    override fun stop() {
        log.info("Stopping saga runtime for ${sagaDef.type.name}")
        runBlocking {
            log.info("Canceling coroutines, isActive=${this@PulsarProtobufSagaRuntime.coroutineContext.isActive}")
            this@PulsarProtobufSagaRuntime.coroutineContext.job.cancelAndJoin()
        }
        log.info("Saga runtime for ${sagaDef.type.name} stopped")
    }

    private fun convertMsg(payloadAndHeaders: PayloadAndHeaders<GeneratedMessageV3>): MessageEnvelope? = when {
        PulsarProtobufCommandProtocol.isCommand(null, payloadAndHeaders) -> {
            PulsarProtobufCommandProtocol.decode(null, payloadAndHeaders)
        }
        PulsarProtobufEventProtocol.isEvent(null, payloadAndHeaders) -> {
            PulsarProtobufEventProtocol.decode(null, payloadAndHeaders)
        }
        else -> {
            log.error(
                "Unsupported message format, discarding. HEADERS=${payloadAndHeaders.headers} MSG=${
                    JsonFormat.printer().print(payloadAndHeaders.payload)
                }."
            )
            null
        }
    }

    private fun getSchema(fullTopic: String): Schema<GeneratedMessageV3> {
        val tenantNs = fullTopic.substringBeforeLast('/')
        val topic = fullTopic.substringAfterLast('/')
        return schemas[tenantNs]!![topic]!!
    }

}
