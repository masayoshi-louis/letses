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
import org.letses.command.BasicCommandEnvelope
import org.letses.command.CommandEnvelope
import org.letses.command.CommandHeading
import org.letses.command.toBasic
import java.time.Instant

object PulsarProtobufCommandProtocol : CommandProtocol<PayloadAndHeaders<out GeneratedMessageV3>> {

    private const val HEADER_TARGET_ID = "Tgt-Id"
    private const val HEADER_COMMAND_ID = "Cmd-Id"
    private const val HEADER_EXPECTED_VERSION = "Expect-Ver"
    private const val HEADER_CORRELATION_ID = "Co-Id"
    private const val HEADER_TIMESTAMP = "Ts"
    private const val HEADER_PARTITION_KEY = "Part-Key"
    private const val HEADER_SAGA_CONTEXT = "Saga-Ctx"

    override fun encode(e: CommandEnvelope): PayloadAndHeaders<out GeneratedMessageV3> {
        require(e.payload is GeneratedMessageV3) { "unsupported command payload format" }
        val headers = mutableMapOf(
            HEADER_TARGET_ID to e.heading.targetId,
            HEADER_COMMAND_ID to e.heading.commandId,
            HEADER_EXPECTED_VERSION to e.heading.expectedVersion.toString(),
            HEADER_CORRELATION_ID to e.heading.correlationId,
            HEADER_TIMESTAMP to e.heading.timestamp.toString()
        )
        e.heading.partitionKey?.let { headers.put(HEADER_PARTITION_KEY, it) }
        e.heading.sagaContext?.let { headers.put(HEADER_SAGA_CONTEXT, it) }
        return PayloadAndHeaders(headers, e.payload as GeneratedMessageV3)
    }

    override fun decode(
        key: String?,
        record: PayloadAndHeaders<out GeneratedMessageV3>
    ): CommandEnvelope {
        val h = CommandHeading.Builder().apply {
            record.headers.forEach { (k, v) ->
                when (k) {
                    HEADER_TARGET_ID -> targetId = v
                    HEADER_COMMAND_ID -> commandId = v
                    HEADER_EXPECTED_VERSION -> expectedVersion = v.toLong()
                    HEADER_CORRELATION_ID -> correlationId = v
                    HEADER_TIMESTAMP -> timestamp = Instant.parse(v)
                    HEADER_PARTITION_KEY -> partitionKey = v
                    HEADER_SAGA_CONTEXT -> sagaContext = v
                    else -> {
                        // ignore
                    }
                }
            }
        }
        return BasicCommandEnvelope(h.build().toBasic(), record.payload)
    }

    fun isCommand(
        key: String?,
        record: PayloadAndHeaders<out GeneratedMessageV3>
    ): Boolean = record.headers.containsKey(HEADER_COMMAND_ID) && record.headers.containsKey(HEADER_TARGET_ID)

}