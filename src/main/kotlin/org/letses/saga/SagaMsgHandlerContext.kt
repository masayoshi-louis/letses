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

package org.letses.saga

import org.letses.platform.MsgHandlerContext

/**
 * For building saga event with contextual information.
 */
interface SagaMsgHandlerContext<T : Trigger> : MsgHandlerContext {

    val selfId: String

    fun newEventBuilder(): SagaEvent.Builder<T>

    fun buildEvent(block: SagaEvent.Builder<T>.() -> Unit): SagaEvent<T> = newEventBuilder().apply(block).build()

}
