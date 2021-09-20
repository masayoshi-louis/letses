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

import com.google.protobuf.GeneratedMessageV3
import org.letses.persistence.PersistentEventEnvelope
import java.time.Instant

object PulsarProtobufEventProtocol :
    EventProtocol<WrappedProtobufEvent, PayloadAndHeaders<out GeneratedMessageV3>> {

    private const val HEADER_SOURCE_ID = "Src-Id"
    private const val HEADER_EVENT_ID = "Ev-Id"
    private const val HEADER_EVENT_VERSION = "Ev-Ver"
    private const val HEADER_CAUSALITY_ID = "Cause-Id"
    private const val HEADER_CORRELATION_ID = "Co-Id"
    private const val HEADER_TIMESTAMP = "Ts"
    private const val HEADER_PARTITION_KEY = "Part-Key"
    private const val HEADER_SAGA_CONTEXT = "Saga-Ctx"

    private const val IGNORE_HEADER_TRACE_ID = "uber-trace-id"

    override fun encode(e: EventEnvelope<WrappedProtobufEvent>): PayloadAndHeaders<out GeneratedMessageV3> {
        val headers = mutableMapOf(
            HEADER_SOURCE_ID to e.heading.sourceId,
            HEADER_EVENT_ID to e.heading.eventId,
            HEADER_EVENT_VERSION to e.heading.version.toString(),
            HEADER_CAUSALITY_ID to e.heading.causalityId,
            HEADER_CORRELATION_ID to e.heading.correlationId,
            HEADER_TIMESTAMP to e.heading.timestamp.toString()
        )
        e.heading.partitionKey?.let { headers.put(HEADER_PARTITION_KEY, it) }
        e.heading.sagaContext?.let { headers.put(HEADER_SAGA_CONTEXT, it) }
        e.heading.extra.forEach { (k, v) -> headers[k] = v }
        return PayloadAndHeaders(headers, e.payload.message)
    }

    override fun decode(
        key: String?,
        record: PayloadAndHeaders<out GeneratedMessageV3>
    ): EventEnvelope<WrappedProtobufEvent> {
        val h = EventHeading.Builder().apply {
            record.headers.forEach { (k, v) ->
                when (k) {
                    HEADER_SOURCE_ID -> sourceId = v
                    HEADER_EVENT_ID -> eventId = v
                    HEADER_EVENT_VERSION -> version = v.toLong()
                    HEADER_CAUSALITY_ID -> causalityId = v
                    HEADER_CORRELATION_ID -> correlationId = v
                    HEADER_TIMESTAMP -> timestamp = Instant.parse(v)
                    HEADER_PARTITION_KEY -> partitionKey = v
                    HEADER_SAGA_CONTEXT -> sagaContext = v
                    IGNORE_HEADER_TRACE_ID -> {
                        // ignore
                    }
                    else -> extra = extra.put(k, v)
                }
            }
        }
        return PersistentEventEnvelope(h.build().toBasic(), WrappedProtobufEvent(record.payload))
    }

    fun isEvent(
        key: String?,
        record: PayloadAndHeaders<out GeneratedMessageV3>
    ): Boolean = record.headers.containsKey(HEADER_SOURCE_ID) && record.headers.containsKey(HEADER_EVENT_ID)

}