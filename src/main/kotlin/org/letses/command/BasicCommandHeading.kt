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

package org.letses.command

import org.letses.eventsourcing.AnyVersion
import org.letses.eventsourcing.EventVersion
import org.letses.messaging.EventEnvelope
import org.letses.utils.newUUID
import java.time.Instant

data class BasicCommandHeading(
    override val targetId: String,
    override val partitionKey: String? = null,
    override val commandId: String = newUUID(),
    override val correlationId: String = newUUID(),
    override val timestamp: Instant = Instant.now(),
    override val sagaContext: String? = null,
    override val expectedVersion: EventVersion = AnyVersion
) : CommandHeading {
    companion object {
        fun from(event: EventEnvelope<*>, targetId: String): BasicCommandHeading =
            BasicCommandHeading(
                targetId = targetId,
                partitionKey = event.heading.partitionKey,
                correlationId = event.heading.correlationId,
                sagaContext = event.heading.sagaContext
            )
    }
}

fun CommandHeading.toBasic(): BasicCommandHeading = this as? BasicCommandHeading ?: BasicCommandHeading(
    targetId = targetId,
    commandId = commandId,
    correlationId = correlationId,
    partitionKey = partitionKey,
    timestamp = timestamp,
    sagaContext = sagaContext,
    expectedVersion = expectedVersion
)
