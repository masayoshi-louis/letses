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

import org.letses.command.CommandHandlerImpl
import org.letses.command.CommandHandlerWithEntityState
import org.letses.command.TracedCommandHandlerImpl
import org.letses.domain.AggregateModel
import org.letses.domain.EventSourcedAggregate
import org.letses.entity.EntityState
import org.letses.entity.EntityStateMachine
import org.letses.messaging.Event
import org.letses.messaging.EventProtocol
import org.letses.persistence.*
import org.letses.persistence.publisher.EventPublisher
import org.letses.persistence.publisher.EventTransform
import org.letses.saga.SagaDef
import org.letses.saga.SagaEvent
import org.letses.saga.Trigger

class PlatformBuilder {
    // Configs
    lateinit var boundedContextName: String
    lateinit var messageBusFactory: MessageBusFactory
    var consistentSnapshotTxManager: ConsistentSnapshotTxManager = ConsistentSnapshotTxManager.PSEUDO
    var tracingEnabled: Boolean = false

    private val aggregates: MutableList<AggregateBuilder<*, *>> = mutableListOf()
    private val sagas: MutableList<SagaBuilder<*, *>> = mutableListOf()

    fun <S : EntityState, E : Event> addAggregate(
        model: AggregateModel<S, E>,
        block: AggregateBuilder<S, E>.() -> Unit
    ) {
        val builder = AggregateBuilder(model)
        builder.block()
        aggregates.add(builder)
    }

    fun <S : EntityStateMachine<S, T>, T : Trigger> addSaga(
        sagaDef: SagaDef<S, T>,
        block: SagaBuilder<S, T>.() -> Unit
    ) {
        val builder = SagaBuilder(sagaDef)
        builder.block()
        sagas.add(builder)
    }

    @Suppress("UNCHECKED_CAST")
    fun build(): Platform {
        require(boundedContextName != "") { "boundedContextName can not be empty" }

        val aggregateMap = aggregates.associate { it.model.type to it.model }
        val aEventStores = aggregates.associate { it.model.type to it.eventStore }

        // setup repositories and command handlers for aggregates
        val aRepositories = aggregates.map {
            val repo = AggregateRepository(
                model = it.model as EventSourcedAggregate<EntityState, Event>,
                eventStore = it.eventStore as EventStore<Event>,
                snapshotStore = it.snapshotStore as SnapshotStore<EntityState>,
                eventPublisher = if (it.eventStore is PassiveEventStore<*>) {
                    messageBusFactory.configureEventPublisher(it.model) as EventPublisher<Event>
                } else {
                    null
                },
                consistentSnapshotTxManager = consistentSnapshotTxManager
            )
            it.model.type to repo
        }
        val aCommandHandlers = aRepositories.associate { (type, repo) ->
            val aggregateModel = aggregateMap[type]!!
            var h: CommandHandlerWithEntityState<*, *> = CommandHandlerImpl(
                model = aggregateModel as AggregateModel<EntityState, Event>,
                repo = repo,
                deduplicationMemSize = aggregateModel.deduplicationMemSize
            )
            if (tracingEnabled) {
                h = TracedCommandHandlerImpl(h)
            }
            type to h
        }

        val aggregatePairs = aggregates.map { Pair(it.model, it.eventStore) }
        messageBusFactory.configureCmdBus(aggregatePairs, aggregateMap, aCommandHandlers, boundedContextName)
        messageBusFactory.configureEventBus(aggregatePairs, aggregateMap, aCommandHandlers, boundedContextName)

        aggregates.forEach {
            when {
                it.configureEventPump != null -> {
                    it.configureEventPump!!.invoke(messageBusFactory)
                }
                it.passiveEventStore != null -> {
                    // TODO
                }
            }
        }

        val createdSagas = sagas.map {
            it.build(messageBusFactory)
        }

        return Platform(
            aggregates = aggregateMap,
            aEventStores = aEventStores,
            aRepositories = aRepositories.toMap(),
            aCommandHandlers = aCommandHandlers,
            messageBus = messageBusFactory.create(),
            sagas = createdSagas
        )
    }

    inner class AggregateBuilder<S : EntityState, E : Event>(val model: AggregateModel<S, E>) {
        lateinit var eventStore: EventStore<E>
        var configureEventPump: ((MessageBusFactory) -> Unit)? = null
        var passiveEventStore: PassiveEventStore<E>? = null
        var snapshotStore: SnapshotStore<S>? = null

        fun withEventStore(eventStore: EventStore<E>) {
            this.eventStore = eventStore
        }

        fun <R : Any> withEventStore(
            eventStore: SubscribableEventStore<E>,
            eventProtocol: EventProtocol<E, R>
        ) {
            this.eventStore = eventStore
            configureEventPump = { messageBusFactory ->
                messageBusFactory.configureEventPump(
                    model,
                    EventTransform.simpleTransform(model.eventChannel, eventProtocol),
                    eventStore,
                    boundedContextName
                )
            }
        }

        fun withEventStore(eventStore: PassiveEventStore<E>) {
            this.eventStore = eventStore
            this.passiveEventStore = eventStore
        }

        fun withSnapshotStore(snapshotStore: SnapshotStore<S>) {
            this.snapshotStore = snapshotStore
        }
    }

    inner class SagaBuilder<S : EntityStateMachine<S, T>, T : Trigger>(private val sagaDef: SagaDef<S, T>) {
        lateinit var eventStore: EventStore<SagaEvent<T>>
        var passiveEventStore: PassiveEventStore<SagaEvent<T>>? = null
        var snapshotStore: SnapshotStore<S>? = null
        private var configureEventPump: ((MessageBusFactory) -> Unit)? = null

        init {
            if (sagaDef.commandChannel != null && sagaDef.commandEnvelopeCls == null)
                throw IllegalArgumentException("commandEnvelopeCls can not be null")
        }

        fun withEventStore(eventStore: EventStore<SagaEvent<T>>) {
            this.eventStore = eventStore
        }

        fun <R : Any> withEventStore(
            eventStore: SubscribableEventStore<SagaEvent<T>>
        ) {
            this.eventStore = eventStore
            configureEventPump = { messageBusFactory ->
                messageBusFactory.configureEventPump(
                    sagaDef,
                    EventTransform.simpleSagaTransform(),
                    eventStore,
                    boundedContextName
                )
            }
        }

        fun withEventStore(eventStore: PassiveEventStore<SagaEvent<T>>) {
            this.eventStore = eventStore
            this.passiveEventStore = eventStore
        }

        fun withSnapshotStore(snapshotStore: SnapshotStore<S>) {
            this.snapshotStore = snapshotStore
        }

        fun build(messageBusFactory: MessageBusFactory): SagaRuntime<S, T> {
            this.configureEventPump?.invoke(messageBusFactory)
            return messageBusFactory.configureSagaRuntime(sagaDef, eventStore, boundedContextName)
        }

    }

}

inline fun newPlatform(block: PlatformBuilder.() -> Unit): Platform {
    val builder = PlatformBuilder()
    builder.block()
    return builder.build()
}
