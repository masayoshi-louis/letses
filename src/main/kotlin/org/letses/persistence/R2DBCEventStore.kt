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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.R2dbcRollbackException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.withContext
import org.letses.eventsourcing.AnyVersion
import org.letses.eventsourcing.EventVersion
import org.letses.eventsourcing.NotExists
import org.letses.messaging.BasicEventHeading
import org.letses.messaging.Event
import org.letses.utils.jackson.registerImmutableCollectionsSerde
import org.slf4j.LoggerFactory
import org.springframework.r2dbc.connection.ConnectionFactoryUtils
import reactor.kotlin.core.publisher.toFlux
import reactor.kotlin.extra.math.sum
import java.io.IOException
import java.time.Instant
import java.util.*

class R2DBCEventStore<E : Event>(
    private val connectionFactory: ConnectionFactory,
    private val tableName: String,
    private val serDe: PersistentEventEnvelope.SerDe<String, E>
) : EventStore<E>, PassiveEventStore<E> {

    companion object {
        private val extraHeadingObjectMapper = ObjectMapper().apply {
            registerKotlinModule()
            registerImmutableCollectionsSerde()
        }

        private val log = LoggerFactory.getLogger(R2DBCEventStore::class.java)
    }

    override suspend fun read(stream: String, from: EventVersion, consumer: (PersistentEventEnvelope<E>) -> Unit): Int {
        val entityId = entityIdFromStreamId(stream)
        var count = 0
        useConnection { conn ->
            conn.createStatement("SELECT * FROM $tableName\nWHERE source_id = $1\nAND version >= $2\nORDER BY version ASC")
                .bind(0, entityId)
                .bind(1, from)
                .execute()
                .toFlux()
                .flatMap { result ->
                    result.map { row, _ ->
                        PersistentEventEnvelope(
                            BasicEventHeading(
                                sourceId = row["source_id", UUID::class.java].toString(),
                                eventId = row["event_id", UUID::class.java].toString(),
                                version = row["version", java.lang.Long::class.java].toLong(),
                                causalityId = row["causality_id", UUID::class.java].toString(),
                                correlationId = row["correlation_id", String::class.java],
                                partitionKey = row["partition_key", String::class.java],
                                timestamp = row["timestamp", Instant::class.java],
                                sagaContext = row["saga_context", String::class.java],
                                extra = extraHeadingObjectMapper.readValue(row["extra_heading", String::class.java])
                            ),
                            serDe.deserializePayload(
                                row["payload", String::class.java],
                                row["payload_type", String::class.java]
                            )
                        )
                    }
                }.asFlow().collect {
                    consumer(it)
                    count += 1
                }
            return count
        }
    }

    override suspend fun append(
        stream: String,
        events: List<PersistentEventEnvelope<E>>,
        expectedVersion: EventVersion
    ) {
        val entityId = entityIdFromStreamId(stream)
        val entityIdStr = entityId.toString()

        // part of transaction, so don't close connection
        useConnection { conn ->
            // check version
            // TODO check continuity
            if (expectedVersion != AnyVersion) {
                val count =
                    conn.createStatement("SELECT COUNT(*) FROM $tableName\nWHERE source_id = $1\nAND version >= $2")
                        .bind(0, entityIdStr)
                        .bind(1, expectedVersion)
                        .execute()
                        .awaitSingle()
                        .map { row, _ ->
                            row.get(0, java.lang.Long::class.java)
                        }.awaitSingle()
                if (count > 1 || (count > 0 && expectedVersion == NotExists)) {
                    throw ConcurrentModificationException("[${stream}] expected version $expectedVersion but $count events found")
                }
            }

            val insertStmt =
                conn.createStatement("INSERT INTO $tableName\n(source_id, version, event_id, timestamp, causality_id, correlation_id, partition_key, saga_context, extra_heading, payload_type, payload, published)\nVALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)")

            for (event in events) {
                require(entityIdStr == event.heading.sourceId)
                insertStmt
                    .bind(0, event.heading.sourceId)
                    .bind(1, event.heading.version)
                    .bind(2, event.heading.eventId)
                    .bind(3, event.heading.timestamp)
                    .bind(4, event.heading.causalityId)
                    .bind(5, event.heading.correlationId)
                if (event.heading.partitionKey == null) {
                    insertStmt.bindNull(6, String::class.java)
                } else {
                    insertStmt.bind(6, event.heading.partitionKey)
                }
                if (event.heading.sagaContext == null) {
                    insertStmt.bindNull(7, String::class.java)
                } else {
                    insertStmt.bind(7, event.heading.sagaContext)
                }
                @Suppress("BlockingMethodInNonBlockingContext")
                insertStmt.bind(8, extraHeadingObjectMapper.writeValueAsString(event.heading.extra))
                insertStmt.bind(9, event.payload.type)
                insertStmt.bind(10, serDe.serializePayload(event.payload))
                insertStmt.bind(11, false)
                insertStmt.add()
            }

            val rowsInserted = insertStmt.execute().toFlux().flatMap { it.rowsUpdated }.sum().awaitSingle()
            if (rowsInserted != events.size.toLong()) {
                throw IOException("expect ${events.size} rows to be inserted, but actually $rowsInserted")
            }
        }
    }

    override suspend fun markEventsAsPublished(stream: String, versionFrom: EventVersion, versionTo: EventVersion) {
        val entityId = entityIdFromStreamId(stream)
        while (true) {
            try {
                useConnection { conn ->
                    val rowsUpdated =
                        conn.createStatement("UPDATE $tableName\nSET published = TRUE\nWHERE source_id = $1\nAND version >= $2\nAND version <= $3")
                            .bind(0, entityId)
                            .bind(1, versionFrom)
                            .bind(2, versionTo)
                            .execute()
                            .awaitSingle()
                            .rowsUpdated
                            .awaitSingle()
                            .toLong()
                    val expected = versionTo - versionFrom + 1
                    if (rowsUpdated != expected) {
                        throw IOException("expect $expected rows to be updated, but actually $rowsUpdated")
                    }
                }
                break
            } catch (e: R2dbcRollbackException) {
                if (e.errorCode == 40001) {
                    log.info("auto retry, exceptionMsg: ${e.message}")
                } else {
                    throw e
                }
            }
        }
    }

    private suspend inline fun <R> useConnection(block: (Connection) -> R): R {
        val conn = ConnectionFactoryUtils.getConnection(connectionFactory).awaitSingle()
        try {
            return block(conn)
        } finally {
            if (conn.isAutoCommit) {
                // not in transaction, so release the connection
                withContext(NonCancellable) {
                    conn.close().awaitFirstOrNull()
                }
            }
            // else: in transaction, should not release the connection or the following operation would fail
        }
    }

    private fun entityIdFromStreamId(stream: String) = stream.substring(stream.indexOf('-') + 1)

}
