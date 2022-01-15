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

import org.letses.command.CommandEnvelope
import java.time.Instant

data class SagaCommand(
    val channel: String,
    val data: String,
    val type: String,
    val key: String? = null,
    val defer: Defer? = null,
    val format: Format = Format.BINARY_BASE64
) {
    companion object {
        internal fun typeStr(cmd: CommandEnvelope) = "${cmd::class.qualifiedName}(${cmd.payload::class.qualifiedName})"

        internal fun payloadType(type: String) =
            type.substring(type.indexOf('(') + 1, type.indexOf(')'))

        @Suppress("unused")
        internal fun typeCls(type: String): Pair<Class<*>, Class<*>> {
            assert(type[type.length - 1] == ')')
            val index = type.indexOf('(')
            val envelopeType = type.substring(0, index)
            val payloadType = type.substring(index + 1, type.length - 1)
            return Class.forName(envelopeType) to Class.forName(payloadType)
        }
    }

    enum class Format {
        BINARY_BASE64
    }

    data class Defer(val time: Instant)
}
