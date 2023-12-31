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

package org.letses.persistence

import kotlinx.coroutines.coroutineScope
import org.letses.entity.EntityState
import org.letses.eventsourcing.EventSourced
import org.letses.eventsourcing.EventVersion
import org.letses.messaging.Event
import org.letses.messaging.EventHeading
import org.letses.persistence.publisher.EventPublisher
import org.letses.platform.MsgHandlerContext
import org.letses.utils.tracing.span
import org.slf4j.LoggerFactory
import java.io.Closeable

abstract class AbstractRepository<S : EntityState, E : Event, in C : MsgHandlerContext>(
    override val model: EventSourced<S, E, C>,
    private val eventStore: EventStore<E>,
    private val snapshotStore: SnapshotStore<S>? = null,
    private val eventPublisher: EventPublisher<E>? = null,
    private val consistentSnapshotTxManager: ConsistentSnapshotTxManager = ConsistentSnapshotTxManager.PSEUDO
) : Repository<S, E, C> {

    companion object {
        private val log = LoggerFactory.getLogger(AbstractRepository::class.java)
    }

    private inner class TransactionImpl(
        private val entityId: String,
        private val correlationId: String,
        msgIdExtractor: (Any) -> String,
        partitionKey: String?,
        deduplicationMemSize: Int,
        enhanceHeading: (EventHeading) -> EventHeading
    ) : AbstractTransaction<S, E, C>(
        model,
        entityId,
        correlationId,
        msgIdExtractor,
        partitionKey,
        deduplicationMemSize,
        enhanceHeading,
        consistentSnapshotTxManager = consistentSnapshotTxManager
    ) {

        private var prevSnapshot: Snapshot<S>? = null

        override suspend fun saveEvents(events: List<PersistentEventEnvelope<E>>, expectedVersion: EventVersion): Unit =
            coroutineScope {
                eventStore.append(streamName(entityId), events, expectedVersion)
                coroutineContext.span?.log("Repository.Transaction.eventsSaved")
            }

        override suspend fun saveSnapshot(takeSnapshot: () -> Snapshot<S>): Unit = coroutineScope {
            snapshotStore?.let { ss ->
                ss.save(entityId, version, prevSnapshot, takeSnapshot)?.let { s ->
                    prevSnapshot = s
                    coroutineContext.span?.log("Repository.Transaction.snapshotSaved")
                    log.debug("<$correlationId>[${model.eventCategory}_$entityId] snapshot saved, ver=${s.version}")
                }
            }
        }

        @Suppress("NAME_SHADOWING")
        override suspend fun publishEvents(events: () -> List<PersistentEventEnvelope<E>>): Unit = coroutineScope {
            publishEvents(entityId, events)
        }

        suspend fun begin(skipLoading: Boolean = false) = coroutineScope {
            coroutineContext.span?.log("Repository.Transaction.begin")
            if (skipLoading) {
                log.debug("<$correlationId>[${model.eventCategory}_$entityId] skipLoading is enabled")
                coroutineContext.span?.log("skip loading")
            } else {
                snapshotStore?.load(entityId)?.let { s ->
                    loadSnapshot(s)
                    coroutineContext.span?.log("Repository.Transaction.snapshotLoaded")
                    prevSnapshot = s
                    log.debug("<$correlationId>[${model.eventCategory}_$entityId] snapshot loaded, ver=${s.version}")
                }
                val eventCount = eventStore.read(streamName(entityId), version + 1, ::applyEvent)
                log.debug("<$correlationId>[${model.eventCategory}_$entityId] $eventCount event(s) replayed")
                coroutineContext.span?.run {
                    log(mapOf("event" to "Repository.Transaction.eventsReplayed", "numEvents" to eventCount))
                }
            }
        }

    }

    override suspend fun beginTransaction(
        settings: TransactionSettings,
        skipLoading: Boolean
    ): Transaction<S, E, C> = TransactionImpl(
        entityId = settings.entityId,
        correlationId = settings.correlationId,
        msgIdExtractor = settings.msgIdExtractor,
        partitionKey = settings.partitionKey,
        deduplicationMemSize = settings.deduplicationMemSize,
        enhanceHeading = settings.enhanceHeading
    ).apply { begin(skipLoading) }

    override fun close() {
        (eventPublisher as? Closeable)?.close()
    }

    override suspend fun salvageUnpublishedEvents() {
        if (eventStore is PassiveEventStore<E> && eventPublisher != null) {
            while (true) {
                var count = 0
                eventStore.loadUnpublishedEvents().collect {
                    count += 1
                    publishEvents(it.heading.sourceId) { listOf(it) }
                }
                if (count == 0) {
                    break
                }
            }
        }
    }

    private fun streamName(id: String) = model.eventCategory + "-" + id

    @Suppress("NAME_SHADOWING")
    private suspend inline fun publishEvents(
        entityId: String,
        crossinline events: () -> List<PersistentEventEnvelope<E>>
    ): Unit = coroutineScope {
        eventPublisher?.run {
            val events = events()
            try {
                publish(events)
                coroutineContext.span?.log("Repository.Transaction.eventsPublished")
                (eventStore as? PassiveEventStore)?.markEventsAsPublished(
                    streamName(entityId),
                    events.minOf { it.heading.version },
                    events.maxOf { it.heading.version }
                )
                coroutineContext.span?.log("Repository.Transaction.eventsMarkedAsPublished")
            } catch (e: Exception) {
                log.error("Publish events failed", e)
            }
        }
    }

}
