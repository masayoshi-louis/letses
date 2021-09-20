/*
 * Copyright (c) 2021 LETSES.org
 *   National Electronics and Computer Technology Center, Thailand
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

package org.letses.persistence.publisher

import com.google.protobuf.GeneratedMessageV3
import io.streamnative.pulsar.tracing.TracingProducerInterceptor
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.letses.messaging.ChannelMapper
import org.letses.messaging.Event
import org.letses.messaging.PayloadAndHeaders
import org.letses.persistence.PersistentEventEnvelope
import org.letses.utils.tracing.injectTracingTo
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class PulsarProtobufEventPublisher<E : Event>(
    private val pulsarClient: PulsarClient,
    private val transform: EventTransformFn<E, PayloadAndHeaders<out GeneratedMessageV3>>,
    private val useTxn: Boolean,
    private val tracingEnabled: Boolean,
    private val channelMapper: ChannelMapper
) : EventPublisher<E>, Closeable {
    companion object {
        private val log = LoggerFactory.getLogger(PulsarProtobufEventPublisher::class.java)
    }

    private val producers = ConcurrentHashMap<String, Producer<in GeneratedMessageV3>>()

    @Suppress("UNCHECKED_CAST", "BlockingMethodInNonBlockingContext")
    override suspend fun publish(events: List<PersistentEventEnvelope<E>>) = coroutineScope {
        val txn = if (useTxn) {
            pulsarClient.newTransaction().build().await()
        } else {
            null
        }
        val futures = events.flatMap { pe ->
            transform(pe).map { (topic, key, value) ->
                val finalTopic = channelMapper(topic)
                val producer = producers.computeIfAbsent(topic) {
                    log.info("Creating producer for topic \"${finalTopic}\"")
                    pulsarClient.newProducer(Schema.PROTOBUF(value.payload::class.java))
                        .topic(finalTopic)
                        .sendTimeout(0, TimeUnit.SECONDS)
                        .apply {
                            if (tracingEnabled) {
                                intercept(TracingProducerInterceptor())
                            }
                        }
                        .create() as Producer<in GeneratedMessageV3>
                }
                val message = if (txn == null) {
                    producer.newMessage()
                } else {
                    producer.newMessage(txn)
                }
                if (tracingEnabled) {
                    injectTracingTo(message)
                }
                key?.let { message.key(it) }
                message.properties(value.headers).value(value.payload).sendAsync()
            }
        }
        futures.forEach { it.await() }
        txn?.commit()?.await()
        if (log.isDebugEnabled) {
            log.debug(
                "<${
                    events.map { it.heading.correlationId }.distinct().joinToString(",")
                }> ${events.size} event(s) published"
            )
        }
    }

    override fun close() {
        log.info("Closing producers")
        producers.values.forEach { it.close() }
        log.info("All producers closed")
    }

}
