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
import org.letses.command.CommandHandlerWithEntityState
import org.letses.domain.AggregateModel
import org.letses.domain.AggregateMsgHandlerContext
import org.letses.domain.AggregateType
import org.letses.entity.EntityState
import org.letses.messaging.Event
import org.letses.persistence.EventStore
import org.letses.persistence.Repository
import org.slf4j.LoggerFactory
import java.io.Closeable

interface Platform {
    fun <E : Event> commandHandlerFor(type: AggregateType<*, E>): CommandHandler<E>
    fun <S : EntityState, E : Event> commandHandlerWithEntityStateFor(type: AggregateType<S, E>): CommandHandlerWithEntityState<S, E>
    fun stop(): Unit
    fun start(): Unit
}

class PlatformImpl internal constructor(
    private val aggregates: Map<AggregateType<*, *>, AggregateModel<*, *>>,
    private val aEventStores: Map<AggregateType<*, *>, EventStore<*>>,
    private val aRepositories: Map<AggregateType<*, *>, Repository<*, *, AggregateMsgHandlerContext>>,
    private val aCommandHandlers: Map<AggregateType<*, *>, CommandHandler<*>>,
    private val messageBus: MessageBus,
    private val sagas: List<SagaRuntime<*, *>>,
) : Platform, Closeable {

    companion object {
        val log = LoggerFactory.getLogger(Platform::class.java)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <E : Event> commandHandlerFor(type: AggregateType<*, E>): CommandHandler<E> =
        aCommandHandlers[type]!! as CommandHandler<E>

    @Suppress("UNCHECKED_CAST")
    override fun <S : EntityState, E : Event> commandHandlerWithEntityStateFor(type: AggregateType<S, E>): CommandHandlerWithEntityState<S, E> =
        aCommandHandlers[type]!! as CommandHandlerWithEntityState<S, E>

    override fun start(): Unit = synchronized(this) {
        messageBus.start()
        sagas.forEach { it.start() }
    }

    override fun stop(): Unit = synchronized(this) {
        sagas.forEach { it.stop() }
        aRepositories.values.forEach { it.close() }
        messageBus.stop()
    }

    override fun close() {
        stop()
    }

}
