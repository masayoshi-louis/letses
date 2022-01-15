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

package org.letses.persistence.publisher

import com.fasterxml.jackson.module.kotlin.readValue
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.util.JsonFormat
import org.letses.messaging.*
import org.letses.persistence.PersistentEventEnvelope
import org.letses.platform.PulsarProtobufSagaRuntime
import org.letses.saga.SagaCommand
import org.letses.saga.SagaEvent
import java.lang.reflect.Method
import java.util.*
import java.util.concurrent.ConcurrentHashMap

// return triple: (channel, key?, value)
internal typealias EventTransformFn<E, R> = (PersistentEventEnvelope<E>) -> Iterable<Triple<String, String?, R>>

object EventTransform {

    fun <E : Event, R> simpleTransform(ch: String, protocol: EventProtocol<E, R>): EventTransformFn<E, R> = {
        val v = protocol.encode(it)
        val k = it.heading.partitionKey ?: it.heading.sourceId
        listOf(Triple(ch, k, v))
    }

    fun <E : Event, R> customKeyTransform(
        ch: String,
        protocol: EventProtocol<E, Pair<String, R>>
    ): EventTransformFn<E, R> = {
        val (k, v) = protocol.encode(it)
        listOf(Triple(ch, k, v))
    }

    fun <E : Event, R> pulsarTransform(ch: String, protocol: EventProtocol<E, R>): EventTransformFn<E, R> = {
        val topic = "${ch}/${it.payload.type}"
        val v = protocol.encode(it)
        val k = it.heading.partitionKey ?: it.heading.sourceId
        listOf(Triple(topic, k, v))
    }

    fun pulsarProtobufTransform(ch: String):
            EventTransformFn<
                    WrappedProtobufEvent,
                    PayloadAndHeaders<out GeneratedMessageV3>
                    > = pulsarTransform(ch, PulsarProtobufEventProtocol)

    fun simpleSagaTransform(): EventTransformFn<SagaEvent<*>, ByteArray> = {
        it.payload.sendCommands.map { c ->
            Triple(c.channel, c.key, Base64.getDecoder().decode(c.data))
        }
    }

    fun pulsarProtobufSagaTransform(): EventTransformFn<SagaEvent<*>, PayloadAndHeaders<out GeneratedMessageV3>> {
        val builderMethods = ConcurrentHashMap<String, Method>()

        fun parseHeaders(c: SagaCommand): Pair<Map<String, String>, Int> {
            val len = c.data.substring(0, PulsarProtobufSagaRuntime.LENGTH_FIELD_LENGTH).toInt()
            val payloadStart = PulsarProtobufSagaRuntime.LENGTH_FIELD_LENGTH + len
            val json = c.data.substring(
                PulsarProtobufSagaRuntime.LENGTH_FIELD_LENGTH,
                payloadStart
            )
            return Pair(PulsarProtobufSagaRuntime.headersObjectMapper.readValue(json), payloadStart)
        }

        fun parsePayload(c: SagaCommand, i: Int): GeneratedMessageV3 {
            val m = builderMethods.computeIfAbsent(SagaCommand.payloadType(c.type)) { name ->
                Class.forName(name).getDeclaredMethod("newBuilder")
            }
            val builder = m.invoke(null) as GeneratedMessageV3.Builder<*>
            val json = c.data.substring(i)
            JsonFormat.parser().merge(json, builder)
            return builder.build() as GeneratedMessageV3
        }

        return {
            it.payload.sendCommands.map { c ->
                val (header, payloadStart) = parseHeaders(c)
                val payload = parsePayload(c, payloadStart)
                val topic = "${c.channel}/${payload::class.java.simpleName}"
                Triple(topic, c.key, PayloadAndHeaders(header, payload))
            }
        }
    }

}

