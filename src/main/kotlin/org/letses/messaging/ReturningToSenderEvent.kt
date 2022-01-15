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

/**
 * This event is published when a command can not be delivered to the target aggregate.
 */
data class ReturningToSenderEvent(
    val reason: Reason,
    /**
     * Full qualified aggregate model class name
     */
    val aggregateClass: String,
    val commandClass: String
) : Event() {
    enum class Reason {
        TargetAggregateNotInitialized
    }
}

data class ReturningToSenderEventEnvelope(
    override val heading: BasicEventHeading,
    override val payload: ReturningToSenderEvent
) : EventEnvelope<ReturningToSenderEvent> {
    companion object
}
