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

package org.letses.persistence

import org.letses.entity.EntityState
import org.letses.eventsourcing.EventVersion
import org.letses.messaging.Event
import org.letses.messaging.EventEnvelope
import org.letses.platform.MsgHandlerContext

/**
 * NOT thread-safe
 */
interface Transaction<out S : EntityState, out E : Event, in C : MsgHandlerContext> {

    fun handleMsg(msg: Any, ctx: C): EventEnvelope<E>?

    val isEntityExists: Boolean

    val state: S

    val version: EventVersion

    fun checkVersion(expected: EventVersion)

    suspend fun commit(): List<PersistentEventEnvelope<E>>

    fun lastCommittedEvents(): List<PersistentEventEnvelope<E>>

}
