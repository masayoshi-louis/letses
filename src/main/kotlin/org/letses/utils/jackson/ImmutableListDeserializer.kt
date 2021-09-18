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
import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.toImmutableList

class ImmutableListDeserializer(private val t: JavaType?) : StdDeserializer<ImmutableList<*>>(t),
    ContextualDeserializer {
    constructor() : this(null) {}

    override fun createContextual(ctxt: DeserializationContext, property: BeanProperty?): JsonDeserializer<*> {
        return if (property == null) {
            this
        } else {
            val t = ctxt.typeFactory.constructCollectionType(List::class.java, property.type.contentType)
            ImmutableListDeserializer(t)
        }
    }

    override fun deserialize(jp: JsonParser, ctxt: DeserializationContext): ImmutableList<*> {
        val list = if (t == null) {
            jp.readValueAs(List::class.java)
        } else {
            jp.codec.readValue<List<*>>(jp, t)
        }
        return list.toImmutableList()
    }
}
