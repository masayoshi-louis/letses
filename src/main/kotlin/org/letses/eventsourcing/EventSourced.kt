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

package org.letses.eventsourcing

import org.letses.entity.EntityState
import org.letses.messaging.Deduplication
import org.letses.messaging.Event
import org.letses.messaging.EventEnvelope
import org.letses.platform.MsgHandlerContext
import kotlin.reflect.full.findAnnotation

interface EventSourced<S : EntityState, E : Event, in C : MsgHandlerContext> {
    val emptyState: S

    fun applyEvent(state: S, event: EventEnvelope<E>): S

    fun handleMsg(state: S, msg: Any, ctx: C): E?

    val eventCategory: String get() = this::class.simpleName!!

    val deduplicationMemSize: Int get() = this::class.findAnnotation<Deduplication>()?.memSize ?: 0
}
