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

package org.letses.platform

import kotlinx.collections.immutable.ImmutableList
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

class PulsarProtobufMessageBus(
    override val coroutineContext: CoroutineContext,
    private val consumerLaunchers: ImmutableList<Runnable>
) : MessageBus, CoroutineScope {

    companion object {
        private val log = LoggerFactory.getLogger(PulsarProtobufMessageBus::class.java)
    }

    override fun start() {
        log.info("Starting PulsarProtobufMessageBus")
        consumerLaunchers.forEach { it.run() }
    }

    override fun stop() {
        log.info("Stopping PulsarProtobufMessageBus")
        runBlocking {
            log.info("Canceling coroutines, isActive=${this@PulsarProtobufMessageBus.coroutineContext.isActive}")
            this@PulsarProtobufMessageBus.coroutineContext.job.cancelAndJoin()
        }
        log.info("PulsarProtobufMessageBus stopped")
    }

}