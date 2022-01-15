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

import org.letses.entity.EntityState
import org.letses.eventsourcing.EventSourced
import org.letses.messaging.Event

interface EventSourcedAggregate<S : EntityState, E : Event> : EventSourced<S, E, AggregateMsgHandlerContext> {

    override fun handleMsg(state: S, msg: Any, ctx: AggregateMsgHandlerContext): E? = handleMsg(state, msg)

    fun handleMsg(state: S, msg: Any): E? = null

}
