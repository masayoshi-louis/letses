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

package org.letses.utils.jackson

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.BeanProperty
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.deser.ContextualDeserializer
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.toImmutableMap

class ImmutableMapDeserializer(private val t: JavaType?) : StdDeserializer<ImmutableMap<*, *>>(t),
    ContextualDeserializer {
    constructor() : this(null) {}

    override fun createContextual(ctxt: DeserializationContext, property: BeanProperty?): JsonDeserializer<*> {
        return if (property == null) {
            this
        } else {
            val kt = property.type.keyType
            val vt = property.type.contentType
            val t = ctxt.typeFactory.constructMapType(Map::class.java, kt, vt)
            ImmutableMapDeserializer(t)
        }
    }

    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): ImmutableMap<*, *> {
        val map = if (t == null) {
            jp.readValueAs(Map::class.java)
        } else {
            jp.codec.readValue<Map<*, *>>(jp, t)
        }
        return map.toImmutableMap()
    }
}
