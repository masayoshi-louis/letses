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

import org.letses.command.CommandHandler
import org.letses.domain.AggregateModel
import org.letses.domain.AggregateType
import org.letses.entity.EntityState
import org.letses.entity.EntityStateMachine
import org.letses.eventsourcing.EventSourced
import org.letses.messaging.Event
import org.letses.persistence.EventStore
import org.letses.persistence.SubscribableEventStore
import org.letses.persistence.publisher.EventPublisher
import org.letses.persistence.publisher.EventTransformFn
import org.letses.saga.SagaDef
import org.letses.saga.SagaEvent
import org.letses.saga.Trigger


interface MessageBusFactory {

    fun <S : EntityStateMachine<S, T>, T : Trigger> configureSagaRuntime(
        sagaDef: SagaDef<S, T>,
        eventStore: EventStore<SagaEvent<T>>,
        boundedContextName: String
    ): SagaRuntime<S, T>

    fun configureCmdBus(
        aggregates: List<Pair<AggregateModel<*, *>, EventStore<*>>>,
        aggregateMap: Map<AggregateType<out EntityState, out Event>, AggregateModel<*, *>>,
        commandHandlers: Map<AggregateType<out EntityState, out Event>, CommandHandler<*>>,
        boundedContextName: String
    )

    fun configureEventBus(
        aggregates: List<Pair<AggregateModel<*, *>, EventStore<*>>>,
        aggregateMap: Map<AggregateType<out EntityState, out Event>, AggregateModel<*, *>>,
        commandHandlers: Map<AggregateType<out EntityState, out Event>, CommandHandler<*>>,
        boundedContextName: String
    )

    fun <E : Event, R : Any> configureEventPump(
        model: EventSourced<*, E, *>,
        transformFn: EventTransformFn<E, R>,
        eventStore: SubscribableEventStore<E>,
        boundedContextName: String
    )

    fun <E : Event> configureEventPublisher(aggregate: AggregateModel<*, E>): EventPublisher<E>

    fun create(): MessageBus

}
