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

import kotlin.reflect.full.findAnnotation

/**
 * Messaging configuration for aggregate.
 */
interface MessageChannelsConfig {
    val eventChannel: String

    val commandChannel: String? get() = null

    val subscribedChannels: List<String> get() = emptyList()

    val channelMapping: (String?) -> String? get() = { it }

    interface AnnotationBased : MessageChannelsConfig {
        override val eventChannel: String
            get() =
                channelMapping(this::class.findAnnotation<EventChannel>()?.channel)!!

        override val commandChannel: String?
            get() =
                channelMapping(this::class.findAnnotation<CommandChannel>()?.channel)

        override val subscribedChannels: List<String>
            get() = (this::class.findAnnotation<Subscribe>()?.channels?.toList() ?: emptyList())
    }
}
