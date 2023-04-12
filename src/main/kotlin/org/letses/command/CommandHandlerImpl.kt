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

package org.letses.command

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import org.letses.domain.AggregateModel
import org.letses.domain.AggregateMsgHandlerContext
import org.letses.domain.CoroutineAggregateMsgHandlerContext
import org.letses.entity.AutoInitialize
import org.letses.entity.EntityState
import org.letses.eventsourcing.AnyVersion
import org.letses.messaging.Event
import org.letses.messaging.EventEnvelope
import org.letses.messaging.toBasic
import org.letses.persistence.Repository
import org.letses.persistence.Transaction
import org.letses.persistence.TransactionSettings
import org.slf4j.LoggerFactory

internal class CommandHandlerImpl<S : EntityState, E : Event>(
    private val model: AggregateModel<S, E>,
    private val repo: Repository<S, E, AggregateMsgHandlerContext>,
    private val exceptionMapper: (Exception) -> Exception = { it },
    private val deduplicationMemSize: Int = 0,
    private val autoInitialize: AutoInitialize<S>? = model.autoInitialize
) : CommandHandlerWithEntityState<S, E> {

    companion object {
        private val log = LoggerFactory.getLogger(CommandHandlerImpl::class.java)
    }

    override val aggregateClass = repo.model::class

    override suspend fun handle(envelope: CommandEnvelope): EventEnvelope<E>? {
        try {
            return doTxn(envelope).lastEvents().lastOrNull()
        } catch (e: Exception) {
            throw exceptionMapper(e)
        }
    }

    override suspend fun handleCommandReturningState(envelope: CommandEnvelope): Pair<EventEnvelope<E>?, S> {
        try {
            val tx = doTxn(envelope)
            val evt = tx.lastEvents().lastOrNull()
            return Pair(evt, tx.state)
        } catch (e: Exception) {
            throw exceptionMapper(e)
        }
    }

    private suspend fun doTxn(envelope: CommandEnvelope): Transaction<S, E, AggregateMsgHandlerContext> =
        coroutineScope {
            val h = envelope.heading
            log.debug("<${h.correlationId}> processing command ${envelope.payload::class.simpleName}(${h.commandId})")
            val txSettings = TransactionSettings(
                partitionKey = h.partitionKey,
                entityId = h.targetId,
                correlationId = h.correlationId,
                msgIdExtractor = { h.commandId },
                deduplicationMemSize = deduplicationMemSize,
                enhanceHeading = {
                    if (h.sagaContext == null) it
                    else it.toBasic().copy(sagaContext = h.sagaContext)
                }
            )
            val isInitMsg = model.isInitialMessage(envelope.payload)
            val tx = repo.beginTransaction(txSettings, skipLoading = isInitMsg && model.allowFastInitialize)
            if (h.expectedVersion != AnyVersion) {
                tx.checkVersion(h.expectedVersion)
            }
            when {
                !tx.isEntityExists && autoInitialize != null -> {
                    tx.handleMsg(autoInitialize.initMsg(h.targetId), CoroutineAggregateMsgHandlerContext(this))
                }

                !tx.isEntityExists && !isInitMsg -> {
                    throw NotInitializedException()
                }

                tx.isEntityExists && isInitMsg -> {
                    throw AlreadyInitializedException()
                }
            }
            tx.handleMsg(envelope.payload, CoroutineAggregateMsgHandlerContext(this))
            try {
                tx.commit()
                tx
            } catch (_: ConcurrentModificationException) {
                delay(100)
                doTxn(envelope) // retry
            }
        }

}
