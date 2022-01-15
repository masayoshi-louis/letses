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

import com.google.protobuf.GeneratedMessageV3
import org.apache.pulsar.client.api.Schema
import java.util.concurrent.ConcurrentHashMap

interface PulsarProtobufSchemas {
    fun add(msgCls: Class<out GeneratedMessageV3>)

    operator fun set(key: String, schema: Schema<GeneratedMessageV3>)

    operator fun get(name: String): Schema<GeneratedMessageV3>?

    fun names(): Collection<String>

    fun addAll(other: PulsarProtobufSchemas) {
        other.names().forEach {
            this[it] = other[it]!!
        }
    }

    companion object {
        operator fun invoke(): PulsarProtobufSchemas = object : PulsarProtobufSchemas {
            private val store = ConcurrentHashMap<String, Schema<GeneratedMessageV3>>()

            @Suppress("UNCHECKED_CAST")
            override fun add(msgCls: Class<out GeneratedMessageV3>) {
                store.computeIfAbsent(msgCls.simpleName) {
                    Schema.PROTOBUF(msgCls) as Schema<GeneratedMessageV3>
                }
            }

            override operator fun set(key: String, schema: Schema<GeneratedMessageV3>) {
                store[key] = schema
            }

            override fun get(name: String): Schema<GeneratedMessageV3>? = store[name]

            override fun names(): Collection<String> = store.keys
        }

        inline operator fun invoke(block: PulsarProtobufSchemas.() -> Unit): PulsarProtobufSchemas =
            invoke().apply(block)
    }
}