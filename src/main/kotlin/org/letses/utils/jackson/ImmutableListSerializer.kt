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

package org.letses.utils.jackson

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import kotlinx.collections.immutable.ImmutableList

class ImmutableListSerializer(t: Class<ImmutableList<*>>?) : StdSerializer<ImmutableList<*>>(t) {
    constructor() : this(null) {}

    override fun serialize(v: ImmutableList<*>, jgen: JsonGenerator, provider: SerializerProvider) {
        jgen.writeStartArray(v.size)
        for (i in v) {
            jgen.writeObject(i)
        }
        jgen.writeEndArray()
    }
}
