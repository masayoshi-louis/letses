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

import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.delay
import org.letses.command.CommandEnvelope
import org.letses.command.toBasic
import org.letses.entity.EntityStateMachine
import org.letses.messaging.EventEnvelope
import org.letses.messaging.MessageEnvelope
import org.letses.persistence.Repository
import org.letses.persistence.TransactionSettings

internal class SagaMessageHandlerImpl<S : EntityStateMachine<S, T>, T : Trigger>(
    private val repo: Repository<S, SagaEvent<T>, SagaMsgHandlerContext<T>>,
    private val sagaDef: SagaDef<S, T>,
    private val cmdSerializer: (String, CommandEnvelope) -> String,
    private val exceptionMapper: (Exception) -> Exception = { it }
) : SagaMessageHandler<S, T> {

    override suspend fun handle(msg: Any) {
        try {
            return doTxn(msg)
        } catch (e: Exception) {
            throw exceptionMapper(e)
        }
    }

    private suspend fun doTxn(msg: Any) {
        // resolve association
        val assoc = if (msg is CommandEnvelope && msg::class == sagaDef.commandEnvelopeCls) {
            SagaAssociateResult.Associated(msg.heading.targetId)
        } else {
            sagaDef.resolveAssociation(msg).let {
                when (it) {
                    SagaAssociateResult.FromHeading -> {
                        if (msg is EventEnvelope<*> && msg.heading.sagaContext != null) {
                            val c = SagaContext.parse(msg.heading.sagaContext!!)
                            if (c.type == sagaDef.type.name) SagaAssociateResult.Associated(c.id)
                            else SagaAssociateResult.DropMessage
                        } else SagaAssociateResult.DropMessage
                    }
                    is SagaAssociateResult.TryAssociate -> {
                        if (it.type == sagaDef.type.name) SagaAssociateResult.Associated(it.id)
                        else SagaAssociateResult.DropMessage
                    }
                    else -> it
                }
            }
        }

        val sagaId = when (assoc) {
            is SagaAssociateResult.Associated -> assoc.id
            is SagaAssociateResult.TryAssociateIfExists -> assoc.id
            else -> return
        }

        val txSettings = when (msg) {
            is MessageEnvelope -> {
                val h = msg.heading
                TransactionSettings(
                    partitionKey = h.partitionKey,
                    entityId = sagaId,
                    correlationId = h.correlationId,
                    msgIdExtractor = { h.messageId },
                    deduplicationMemSize = 0
                )
            }
            else -> {
                throw AssertionError("unsupported msg type: ${msg::class.qualifiedName}")
            }
        }
        val tx = repo.beginTransaction(txSettings)

        if (assoc is SagaAssociateResult.TryAssociateIfExists && !tx.isEntityExists) return

        val sagaContext = SagaContext(sagaDef.type.name, txSettings.entityId)

        class SagaEventBuilder : SagaEvent.Builder<T> {
            private var st: T? = null
            private val cmds = mutableListOf<Triple<String, String, CommandEnvelope>>() // (channel, key, data)

            override fun withStateTransfer(trigger: T): SagaEventBuilder {
                st = trigger
                return this
            }

            override fun sendCommand(channel: String, cmd: CommandEnvelope): SagaEventBuilder {
                val h = cmd.heading.toBasic().copy(
                    correlationId = txSettings.correlationId,
                    partitionKey = txSettings.partitionKey,
                    sagaContext = sagaContext.toString()
                )
                val e = cmd.headingUpdated(h)
                val k = h.partitionKey ?: h.targetId
                cmds.add(Triple(channel, k, e))
                return this
            }

            override fun clearCommands() {
                cmds.clear()
            }

            override fun build(): SagaEvent<T> {
                val cmds = this.cmds.map { (ch, k, v) ->
                    SagaCommand(
                        channel = ch,
                        data = cmdSerializer(ch, v),
                        type = SagaCommand.typeStr(v),
                        key = k
                    )
                }
                return SagaEvent(stateTransfer = st, sendCommands = cmds.toImmutableList())
            }
        }

        val ctx = object : SagaMsgHandlerContext<T> {
            override val selfId = sagaId

            override fun newEventBuilder() = SagaEventBuilder()
        }

        tx.handleMsg(msg, ctx)
        try {
            tx.commit()
        } catch (_: ConcurrentModificationException) {
            delay(100)
            doTxn(msg) // retry
        }
    }

}
