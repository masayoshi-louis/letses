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

package org.letses.saga

/**
 * Saga information sent with command.
 *
 * It is passed back in the event which is caused by the command sent by saga.
 *
 * Using this additional information, the runtime can load the correct saga instance to handle the event.
 */
data class SagaContext(
    /**
     * From SagaType.name
     */
    val type: String,
    /**
     * Saga instance ID
     */
    val id: String
) {

    override fun toString(): String = "$type,$id"

    companion object {
        fun parse(str: String): SagaContext {
            val split = str.split(",")
            require(split.size == 2)
            return SagaContext(split[0], split[1])
        }
    }

}
