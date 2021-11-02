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

package org.letses.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule
import com.google.protobuf.GeneratedMessageV3
import com.hubspot.jackson.datatype.protobuf.ProtobufModule
import org.letses.messaging.BasicEventHeading
import org.letses.messaging.Event
import org.letses.messaging.EventEnvelope
import org.letses.messaging.WrappedProtobufEvent
import org.letses.saga.SagaEvent
import org.letses.saga.Trigger
import org.letses.utils.jackson.registerImmutableCollectionsSerde
import kotlin.reflect.full.isSubclassOf

data class PersistentEventEnvelope<out E : Event>(
    override val heading: BasicEventHeading,
    override val payload: E
) : EventEnvelope<E> {
    interface SerDe<A, E : Event> {
        fun serialize(e: PersistentEventEnvelope<E>): A
        fun deserialize(a: A, payloadType: String): PersistentEventEnvelope<E>
        fun serializePayload(payload: E): A
        fun deserializePayload(a: A, payloadType: String): E
    }

    companion object {
        inline fun <reified E : Event> defaultJsonSerDe(): SerDe<String, E> = object : SerDe<String, E> {
            private val cls = E::class
            private val om = ObjectMapper().apply {
                registerKotlinModule()
                registerImmutableCollectionsSerde()
                registerModule(ParameterNamesModule())
                registerModule(Jdk8Module())
                registerModule(JavaTimeModule())
            }

            init {
                val subTypes = cls.nestedClasses.filter { it.isFinal && it.isSubclassOf(cls) }
                for (t in subTypes) {
                    om.registerSubtypes(NamedType(t.java, t.simpleName))
                }
            }

            override fun serialize(e: PersistentEventEnvelope<E>): String = om.writeValueAsString(e)

            override fun deserialize(a: String, payloadType: String): PersistentEventEnvelope<E> = om.readValue(a)

            override fun serializePayload(payload: E): String = om.writeValueAsString(payload)

            override fun deserializePayload(a: String, payloadType: String): E = om.readValue(a)
        }

        inline fun <reified T : Trigger> sagaDefaultJsonSerDe(): SerDe<String, SagaEvent<T>> =
            object : SerDe<String, SagaEvent<T>> {
                private val cls = T::class
                private val om = ObjectMapper().apply {
                    registerKotlinModule()
                    registerImmutableCollectionsSerde()
                    registerModule(ParameterNamesModule())
                    registerModule(Jdk8Module())
                    registerModule(JavaTimeModule())
                }

                init {
                    val subTypes = cls.nestedClasses.filter { it.isFinal && it.isSubclassOf(cls) }
                    for (t in subTypes) {
                        om.registerSubtypes(NamedType(t.java, t.simpleName))
                    }
                }

                override fun serialize(e: PersistentEventEnvelope<SagaEvent<T>>): String = om.writeValueAsString(e)

                override fun deserialize(a: String, payloadType: String): PersistentEventEnvelope<SagaEvent<T>> =
                    om.readValue(a)

                override fun serializePayload(payload: SagaEvent<T>): String = om.writeValueAsString(payload)

                override fun deserializePayload(a: String, payloadType: String): SagaEvent<T> = om.readValue(a)
            }

        fun defaultProtobufSerDe(baseCls: Class<*>): SerDe<String, WrappedProtobufEvent> =
            object : SerDe<String, WrappedProtobufEvent> {
                private val baseName = baseCls.name
                private val objectMapper = ObjectMapper().registerModules(ProtobufModule())

                override fun serialize(e: PersistentEventEnvelope<WrappedProtobufEvent>): String {
                    throw UnsupportedOperationException()
                }

                override fun deserialize(
                    a: String,
                    payloadType: String
                ): PersistentEventEnvelope<WrappedProtobufEvent> {
                    throw UnsupportedOperationException()
                }

                override fun serializePayload(payload: WrappedProtobufEvent): String =
                    objectMapper.writeValueAsString(payload.message)

                override fun deserializePayload(json: String, payloadType: String): WrappedProtobufEvent {
                    val cls = Thread.currentThread().contextClassLoader.loadClass("${baseName}\$$payloadType")
                    val payload = objectMapper.readValue(json, cls)
                    return WrappedProtobufEvent(payload as GeneratedMessageV3)
                }
            }
    }
}
