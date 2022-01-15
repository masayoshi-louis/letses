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

package org.letses.messaging

import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentMapOf
import org.letses.eventsourcing.EventVersion
import java.time.Instant

interface EventHeading : MessageHeading {
    val sourceId: String
    val eventId: String
    val version: EventVersion
    val causalityId: String
    override val correlationId: String
    override val partitionKey: String?
    override val timestamp: Instant
    override val sagaContext: String?
    val extra: ImmutableMap<String, String>
    override val messageId: String get() = eventId

    data class Builder(
        var sourceId: String? = null,
        var eventId: String? = null,
        var version: EventVersion? = null,
        var causalityId: String? = null,
        var correlationId: String? = null,
        var partitionKey: String? = null,
        var timestamp: Instant? = null,
        var sagaContext: String? = null,
        var extra: PersistentMap<String, String> = persistentMapOf()
    ) {
        fun build(): EventHeading = BasicEventHeading(
            sourceId!!,
            eventId!!,
            version!!,
            causalityId!!,
            correlationId!!,
            partitionKey,
            timestamp!!,
            sagaContext,
            extra
        )
    }
}
