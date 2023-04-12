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

package org.letses.domain

import org.letses.command.CommandEnvelope
import org.letses.entity.AutoInitialize
import org.letses.entity.EntityState
import org.letses.messaging.Event
import org.letses.messaging.Initial
import org.letses.messaging.MessageChannelsConfig
import org.letses.messaging.RetryControl
import kotlin.reflect.full.findAnnotation

/**
 * All aggregates (roots) should implement this interface.
 */
interface AggregateModel<S : EntityState, E : Event> : EventSourcedAggregate<S, E>,
    MessageChannelsConfig.AnnotationBased {
    /**
     * Transform the incoming event to a command, which is forward to the command handler.
     *
     * Return null to drop the event.
     */
    suspend fun processForeignEvent(
        event: Any,
        retry: RetryControl,
        emit: suspend (CommandEnvelope) -> Unit
    ) {
    } // default: drop all events

    @Suppress("UNCHECKED_CAST")
    val autoInitialize: AutoInitialize<S>?
        get() = this as? AutoInitialize<S>

    fun isInitialMessage(msg: Any): Boolean =
        msg::class.findAnnotation<Initial>() != null

    /**
     * The type identifier.
     */
    @Suppress("UNCHECKED_CAST")
    val type: AggregateType<S, E>
        get() = this as? AggregateType<S, E> ?: throw NotImplementedError()

    val allowFastInitialize: Boolean
        get() = false
}
