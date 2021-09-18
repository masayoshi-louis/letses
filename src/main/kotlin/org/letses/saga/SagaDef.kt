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

package org.letses.saga

import org.letses.command.CommandEnvelope
import org.letses.entity.EntityStateMachine
import org.letses.messaging.CommandChannel
import org.letses.messaging.EventEnvelope
import org.letses.messaging.Subscribe
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

/**
 * All sagas should implement this interface.
 */
interface SagaDef<S : EntityStateMachine<S, T>, T : Trigger> : EventSourcedSaga<S, T> {

    /**
     * The type identifier.
     */
    @Suppress("UNCHECKED_CAST")
    val type: SagaType<S, T>
        get() = this as? SagaType<S, T> ?: throw NotImplementedError()

    override val eventCategory: String
        get() = type.name

    override fun applyEvent(state: S, event: EventEnvelope<SagaEvent<T>>): S = with(event.payload) {
        if (stateTransfer == null) state
        else state.apply(stateTransfer)
    }

    val subscribedChannels: List<String>
        get() = (this::class.findAnnotation<Subscribe>()?.channels?.toList() ?: emptyList())

    val commandChannel: String? get() = this::class.findAnnotation<CommandChannel>()?.channel

    fun resolveAssociation(msg: Any): SagaAssociateResult

    val commandEnvelopeCls: KClass<out CommandEnvelope>? get() = null

}
