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

package org.letses.persistence

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.SendChannel
import org.letses.messaging.Event

interface SubscribableEventStore<E : Event> : EventStore<E> {
    class SubscriptionDroppedException(message: String?, cause: Throwable?) : RuntimeException(message, cause)

    /**
     * @param from eventNumber or null for beginning
     */
    fun CoroutineScope.subscribeToCategory(
        category: String,
        ch: SendChannel<PersistentEventEnvelope<E>>,
        from: Long? = null
    )
}
