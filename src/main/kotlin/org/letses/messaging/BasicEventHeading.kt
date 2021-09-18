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

package org.letses.messaging

import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.persistentMapOf
import org.letses.eventsourcing.EventVersion
import java.time.Instant

data class BasicEventHeading(
    override val sourceId: String,
    override val eventId: String,
    override val version: EventVersion,
    override val causalityId: String,
    override val correlationId: String,
    override val partitionKey: String? = null,
    override val timestamp: Instant = Instant.now(),
    override val sagaContext: String? = null,
    override val extra: ImmutableMap<String, String> = persistentMapOf()
) : EventHeading

fun EventHeading.toBasic(): BasicEventHeading = this as? BasicEventHeading ?: BasicEventHeading(
    sourceId = sourceId,
    eventId = eventId,
    version = version,
    causalityId = causalityId,
    correlationId = correlationId,
    partitionKey = partitionKey,
    timestamp = timestamp,
    sagaContext = sagaContext,
    extra = extra
)
