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

package org.letses.utils.protobuf

import com.google.protobuf.Duration
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.Timestamp
import java.time.Instant
import kotlin.reflect.KFunction1

/**
 * Usage: MyMsgCls::getProp.getOrNull(msg)
 */
inline fun <reified T : GeneratedMessageV3, V> KFunction1<T, V>.getOrNull(msg: T): V? {
    @Suppress("DEPRECATION", "UNCHECKED_CAST")
    val hasMethod = try {
        T::class.java.getDeclaredMethod("has${this.name.substring(3)}")
    } catch (e: NoSuchMethodException) {
        // not optional field
        return this.invoke(msg)
    }
    return if (hasMethod == null || hasMethod.invoke(msg) as Boolean) {
        this.invoke(msg)
    } else {
        null
    }
}

fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

fun Instant.toProtobuf(): Timestamp = Timestamp.newBuilder().run {
    seconds = this@toProtobuf.epochSecond
    nanos = this@toProtobuf.nano
    build()
}

fun Duration.toJava(): java.time.Duration = java.time.Duration.ofSeconds(seconds, nanos.toLong())

fun java.time.Duration.toProtobuf(): Duration = Duration.newBuilder().run {
    seconds = this@toProtobuf.seconds
    nanos = this@toProtobuf.nano
    build()
}
