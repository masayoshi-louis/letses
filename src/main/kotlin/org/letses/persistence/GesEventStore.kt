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

import com.github.msemys.esjc.*
import com.github.msemys.esjc.operation.WrongExpectedVersionException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.sendBlocking
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import org.letses.eventsourcing.AnyVersion
import org.letses.eventsourcing.EventVersion
import org.letses.eventsourcing.NotExists
import org.letses.messaging.Event
import org.slf4j.LoggerFactory

class GesEventStore<E : Event>(
    private val gesClient: com.github.msemys.esjc.EventStore,
    private val serDe: PersistentEventEnvelope.SerDe<String, E>,
    private val streamPrefix: String = "",
    private val readBatchSize: Int = 100
) : EventStore<E>, SubscribableEventStore<E> {

    private val log = LoggerFactory.getLogger(GesEventStore::class.java)

    override suspend fun read(stream: String, from: EventVersion, consumer: (PersistentEventEnvelope<E>) -> Unit) {
        var fromVar = from
        while (true) {
            val resp = gesClient.readStreamEventsForward(streamPrefix + stream, fromVar, readBatchSize, false).await()
            if (resp.status == SliceReadStatus.StreamDeleted || resp.status == SliceReadStatus.StreamNotFound) {
                break
            }
            for (i in resp.events) {
                val oe = i.originalEvent()
                if (!oe.isJson) throw AssertionError()
                val json = String(oe.data, Charsets.UTF_8)
                consumer(serDe.deserialize(json, oe.eventType))
            }
            if (resp.isEndOfStream) {
                break
            } else {
                fromVar += readBatchSize
            }
        }
    }

    override suspend fun append(
        stream: String,
        events: List<PersistentEventEnvelope<E>>,
        expectedVersion: EventVersion
    ) {
        val data = events.map {
            EventData.newBuilder()
                .type(it.payload.type)
                .jsonMetadata("""{"${'$'}correlationId":"${it.heading.correlationId}"}""")
                .jsonData(serDe.serialize(it))
                .build()
        }
        val expVer = when (expectedVersion) {
            AnyVersion -> ExpectedVersion.ANY
            NotExists -> ExpectedVersion.NO_STREAM
            else -> expectedVersion
        }
        try {
            gesClient.appendToStream(streamPrefix + stream, expVer, data).await()
        } catch (e: WrongExpectedVersionException) {
            throw ConcurrentModificationException(e)
        }
    }

    override fun CoroutineScope.subscribeToCategory(
        category: String,
        ch: SendChannel<PersistentEventEnvelope<E>>,
        from: Long?
    ) {
        val settings = CatchUpSubscriptionSettings.newBuilder()
            .resolveLinkTos(true)
            .build()
        var stopped = false
        val shutdownCh = Channel<Boolean>()
        val stream = "\$ce-$streamPrefix$category"
        val sub = gesClient.subscribeToStreamFrom(stream, from, settings, object : CatchUpSubscriptionListener {
            override fun onEvent(subscription: CatchUpSubscription, dbEvent: ResolvedEvent) {
                if (stopped) return
                try {
                    val e = dbEvent.event
                    assert(e.isJson)
                    val json = String(e.data, Charsets.UTF_8)
                    val v = serDe.deserialize(json, e.eventType)
                    ch.sendBlocking(v) // block current thread for channel send
                } catch (t: Throwable) {
                    ch.close(t)
                    shutdownCh.sendBlocking(true)
                }
            }

            override fun onLiveProcessingStarted(subscription: CatchUpSubscription) {}

            override fun onClose(
                subscription: CatchUpSubscription,
                reason: SubscriptionDropReason,
                exception: Exception?
            ) {
                if (stopped)
                    ch.close()
                else
                    ch.close(SubscribableEventStore.SubscriptionDroppedException(reason.toString(), exception))
                shutdownCh.sendBlocking(true)
            }
        })
        launch {
            try {
                // wait until completed or canceled
                shutdownCh.receive()
            } finally {
                stopped = true
                sub.close()
                log.debug("subscription is closed")
            }
        }
    }

}