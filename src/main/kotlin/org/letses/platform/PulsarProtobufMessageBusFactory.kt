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

package org.letses.platform

import com.google.protobuf.GeneratedMessageV3
import io.github.shinigami.coroutineTracingApi.injectTracing
import io.github.shinigami.coroutineTracingApi.span
import io.opentracing.util.GlobalTracer
import io.streamnative.pulsar.tracing.TracingConsumerInterceptor
import io.streamnative.pulsar.tracing.TracingPulsarUtils
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.persistentMapOf
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import org.apache.pulsar.client.api.*
import org.letses.command.CommandHandler
import org.letses.domain.AggregateModel
import org.letses.domain.AggregateType
import org.letses.entity.EntityState
import org.letses.entity.EntityStateMachine
import org.letses.eventsourcing.EventSourced
import org.letses.messaging.*
import org.letses.persistence.EventStore
import org.letses.persistence.SubscribableEventStore
import org.letses.persistence.publisher.EventPublisher
import org.letses.persistence.publisher.EventTransform
import org.letses.persistence.publisher.EventTransformFn
import org.letses.persistence.publisher.PulsarProtobufEventPublisher
import org.letses.saga.SagaDef
import org.letses.saga.SagaEvent
import org.letses.saga.Trigger
import org.letses.utils.Defer
import org.letses.utils.awaitNoCancel
import org.letses.utils.tracing.span
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

class PulsarProtobufMessageBusFactory : MessageBusFactory, CoroutineScope {
    companion object {
        private val log = LoggerFactory.getLogger(PulsarProtobufMessageBusFactory::class.java)
    }

    override val coroutineContext: CoroutineContext = Job()

    lateinit var pulsarClient: PulsarClient

    var channelMapper: ChannelMapper = ChannelMapper.NOOP

    var useTxn: Boolean = false

    var tracingEnabled: Boolean = false

    var commandListenerEnabled: Boolean = true
    var eventListenerEnabled: Boolean = true

    /**
     * channel (tenant/ns) -> schemas
     */
    private var schemas: PersistentMap<String, PulsarProtobufSchemas> = persistentMapOf()

    private var consumerLaunchers = persistentListOf<Runnable>()

    fun addSchemas(ch: String, schemas: PulsarProtobufSchemas) {
        val current = this.schemas[ch]
        if (current == null) {
            this.schemas = this.schemas.put(ch, schemas)
        } else {
            current.addAll(schemas)
        }
    }

    fun addEventSchemas(model: MessageChannelsConfig, schemas: PulsarProtobufSchemas) {
        addSchemas(model.eventChannel, schemas)
    }

    fun addEventSchemas(model: MessageChannelsConfig, vararg cls: Class<GeneratedMessageV3>) {
        addEventSchemas(model, PulsarProtobufSchemas {
            for (clazz in cls) {
                add(clazz)
            }
        })
    }

    @Suppress("UNCHECKED_CAST")
    fun addEventSchemas(model: MessageChannelsConfig, baseCls: KClass<*>) {
        val subTypes = baseCls.nestedClasses.filter { it.isFinal && it.isSubclassOf(GeneratedMessageV3::class) }
        addEventSchemas(model, *subTypes.map { it.java as Class<GeneratedMessageV3> }.toTypedArray())
    }

    fun addCommandSchemas(model: MessageChannelsConfig, schemas: PulsarProtobufSchemas) {
        addSchemas(model.commandChannel!!, schemas)
    }

    fun addCommandSchemas(model: MessageChannelsConfig, vararg cls: Class<GeneratedMessageV3>) {
        addCommandSchemas(model, PulsarProtobufSchemas {
            for (clazz in cls) {
                add(clazz)
            }
        })
    }

    @Suppress("UNCHECKED_CAST")
    fun addCommandSchemas(model: MessageChannelsConfig, baseCls: KClass<*>) {
        val subTypes = baseCls.nestedClasses.filter { it.isFinal && it.isSubclassOf(GeneratedMessageV3::class) }
        addCommandSchemas(model, *subTypes.map { it.java as Class<GeneratedMessageV3> }.toTypedArray())
    }

    override fun <S : EntityStateMachine<S, T>, T : Trigger> configureSagaRuntime(
        sagaDef: SagaDef<S, T>,
        eventStore: EventStore<SagaEvent<T>>,
        boundedContextName: String
    ): SagaRuntime<S, T> = PulsarProtobufSagaRuntime(
        sagaDef,
        eventStore,
        PulsarProtobufEventPublisher(
            pulsarClient,
            EventTransform.pulsarProtobufSagaTransform(),
            useTxn,
            tracingEnabled,
            channelMapper
        ),
        schemas,
        boundedContextName,
        pulsarClient,
        channelMapper
    )

    override fun configureCmdBus(
        aggregates: List<Pair<AggregateModel<*, *>, EventStore<*>>>,
        aggregateMap: Map<AggregateType<out EntityState, out Event>, AggregateModel<*, *>>,
        commandHandlers: Map<AggregateType<out EntityState, out Event>, CommandHandler<*>>,
        boundedContextName: String
    ) {
        if (!commandListenerEnabled) return

        // create a consumer instance for each command type
        aggregates.forEach { (aggregate, _) ->
            aggregate.commandChannel?.let { cmdCh ->
                schemas[cmdCh]?.let { theSchemas ->
                    theSchemas.names().forEach { type ->
                        val theSchema = theSchemas[type]
                        val topic = channelMapper("$cmdCh/$type")
                        val subName = "$boundedContextName/${aggregate.type.name}"
                        val cmdHandler = commandHandlers[aggregate.type]
                            ?: throw AssertionError("can not find command handler for aggregate ${aggregate.type}")
                        consumerLaunchers = consumerLaunchers.add(Runnable {
                            launch {
                                val consumer = pulsarClient.newConsumer(theSchema)
                                    .topic(topic)
                                    .subscriptionName(subName)
                                    .subscriptionType(SubscriptionType.Key_Shared)
                                    .apply {
                                        if (tracingEnabled) {
                                            intercept(TracingConsumerInterceptor())
                                        }
                                    }
                                    .subscribeAsync()
                                    .await()
                                log.info("Pulsar consumer started, topic=$topic, subscriptionName=$subName")

                                Defer.scope {
                                    defer {
                                        consumer.closeAsync().await()
                                        log.info("Pulsar consumer stopped, topic=$topic, subscriptionName=$subName")
                                    }

                                    while (isActive) {
                                        val msg = consumer.receiveAsync().await()
                                        tracingFrom(msg) {
                                            try {
                                                val cmd = PulsarProtobufCommandProtocol.decode(
                                                    msg.key,
                                                    PayloadAndHeaders(msg.properties, msg.value)
                                                )
                                                span?.log("MessageBus.decoded")
                                                cmdHandler.handle(cmd)
                                                span?.log("MessageBus.processFinished")
                                                consumer.acknowledgeAsync(msg).awaitNoCancel()
                                                span?.log("MessageBus.messageAcknowledged")
                                            } catch (e: Exception) {
                                                log.error("error processing command: $msg", e)
                                                consumer.negativeAcknowledge(msg)
                                            }
                                        }
                                    }
                                }
                            }
                        }) // end: add
                    }
                }
            }
        }
    }

    override fun configureEventBus(
        aggregates: List<Pair<AggregateModel<*, *>, EventStore<*>>>,
        aggregateMap: Map<AggregateType<out EntityState, out Event>, AggregateModel<*, *>>,
        commandHandlers: Map<AggregateType<out EntityState, out Event>, CommandHandler<*>>,
        boundedContextName: String
    ) {
        if (!eventListenerEnabled) return

        aggregates.forEach { (aggregate, _) ->
            aggregate.subscribedChannels.forEach { topic ->
                val theSchema = topicSchema(topic)
                val subName = "$boundedContextName/${aggregate.type.name}"
                val finalTopic = channelMapper(topic)
                val cmdHandler = commandHandlers[aggregate.type]
                    ?: throw AssertionError("can not find command handler for aggregate ${aggregate.type}")
                consumerLaunchers = consumerLaunchers.add(Runnable {
                    launch {
                        val consumer = pulsarClient.newConsumer(theSchema)
                            .topic(finalTopic)
                            .subscriptionName(subName)
                            .subscriptionType(SubscriptionType.Key_Shared)
                            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                            .apply {
                                if (tracingEnabled) {
                                    intercept(TracingConsumerInterceptor())
                                }
                            }
                            .subscribeAsync()
                            .await()
                        log.info("Pulsar consumer started, topic=$finalTopic, subscriptionName=$subName")

                        Defer.scope {
                            defer {
                                consumer.closeAsync().await()
                                log.info("Pulsar consumer stopped, topic=$finalTopic, subscriptionName=$subName")
                            }

                            while (isActive) {
                                val msg = consumer.receiveAsync().await()
                                tracingFrom(msg) {
                                    try {
                                        val event = PulsarProtobufEventProtocol.decode(
                                            msg.key,
                                            PayloadAndHeaders(msg.properties, msg.value)
                                        )
                                        span?.log("MessageBus.decoded")
                                        aggregate.processForeignEvent(
                                            event,
                                            RetryControl.create { consumer.negativeAcknowledge(msg) },
                                            cmdHandler::handle
                                        )
                                        span?.log("MessageBus.processFinished")
                                        consumer.acknowledgeAsync(msg).awaitNoCancel()
                                        span?.log("MessageBus.messageAcknowledged")
                                    } catch (e: Exception) {
                                        log.error("error processing event: $msg", e)
                                        consumer.negativeAcknowledge(msg)
                                    }
                                }
                            }
                        }
                    }
                }) // end: add
            }
        }
    }

    override fun <E : Event, R : Any> configureEventPump(
        model: EventSourced<*, E, *>,
        transformFn: EventTransformFn<E, R>,
        eventStore: SubscribableEventStore<E>,
        boundedContextName: String
    ) {
        throw UnsupportedOperationException()
    }

    @Suppress("UNCHECKED_CAST")
    override fun <E : Event> configureEventPublisher(aggregate: AggregateModel<*, E>): EventPublisher<E> =
        PulsarProtobufEventPublisher(
            pulsarClient,
            EventTransform.pulsarProtobufTransform(aggregate.eventChannel),
            useTxn,
            tracingEnabled,
            channelMapper
        ) as EventPublisher<E>

    override fun create(): MessageBus = PulsarProtobufMessageBus(
        coroutineContext,
        consumerLaunchers
    )

    private fun topicSchema(fullTopic: String): Schema<GeneratedMessageV3> {
        val delimiterIdx = fullTopic.lastIndexOf('/')
        val ch = fullTopic.subSequence(0, delimiterIdx)
        val topic = fullTopic.substring(delimiterIdx + 1)
        val schemas = schemas[ch] ?: throw Exception("no schemas for channel: $ch, please check the configuration")
        return schemas[topic] ?: throw Exception("no schemas for topic: $topic in $ch, please check the configuration")
    }

    private suspend inline fun <T> tracingFrom(
        msg: Message<T>,
        crossinline handle: suspend CoroutineScope.(Message<T>) -> Unit
    ) {
        val spanContext = if (tracingEnabled) {
            TracingPulsarUtils.extractSpanContext(msg, GlobalTracer.get())
        } else {
            null
        }
        if (spanContext != null) {
            injectTracing(GlobalTracer.get(), {
                span("handleMsg") {
                    asChildOf(spanContext)
                }
            }) {
                handle(msg)
            }
        } else {
            coroutineScope {
                handle(msg)
            }
        }
    }

}