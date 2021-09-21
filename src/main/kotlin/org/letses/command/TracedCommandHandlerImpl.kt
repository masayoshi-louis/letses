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

package org.letses.command

import io.github.shinigami.coroutineTracingApi.withTrace
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import org.letses.domain.AggregateModel
import org.letses.entity.EntityState
import org.letses.messaging.Event
import org.letses.messaging.EventEnvelope
import org.letses.utils.tracing.span
import org.letses.utils.tracing.traced


class TracedCommandHandlerImpl<S : EntityState, E : Event>(
    private val boundedContextName: String,
    private val model: AggregateModel<S, E>,
    private val inner: CommandHandlerWithEntityState<S, E>
) : CommandHandler<E> by inner, CommandHandlerWithEntityState<S, E> {

    companion object {
        const val TAG_COMPONENT = "component"
        const val TAG_AGGREGATE_TYPE = "aggregate.type"
        const val TAG_AGGREGATE_ID = "aggregate.id"
        const val TAG_MSG_ID = "message.id"
        const val TAG_MSG_ENVELOPE_TYPE = "message.envelopeType"
        const val TAG_MSG_PAYLOAD_TYPE = "message.payloadType"
        const val TAG_MSG_SAGA_CTX = "message.sagaContext"
        const val TAG_CMD_EXPECT_VER = "command.expectedVersion"
    }

    override suspend fun handle(envelope: CommandEnvelope): EventEnvelope<E>? = coroutineScope {
        if (traced) {
            withTrace(opName(envelope)) {
                tagSpan(envelope)
                inner.handle(envelope)
            }
        } else {
            inner.handle(envelope)
        }
    }

    override suspend fun handleCommandReturningState(envelope: CommandEnvelope): Pair<EventEnvelope<E>?, S> =
        coroutineScope {
            if (traced) {
                withTrace(opName(envelope)) {
                    tagSpan(envelope)
                    inner.handleCommandReturningState(envelope)
                }
            } else {
                inner.handleCommandReturningState(envelope)
            }
        }

    private fun CoroutineScope.tagSpan(envelope: CommandEnvelope) {
        span?.setTag(TAG_COMPONENT, "$boundedContextName/${model.type.name}")
        span?.setTag(TAG_AGGREGATE_TYPE, model::class.qualifiedName)
        span?.setTag(TAG_AGGREGATE_ID, envelope.heading.targetId)
        span?.setTag(TAG_MSG_ID, envelope.heading.commandId)
        span?.setTag(TAG_MSG_ENVELOPE_TYPE, envelope::class.qualifiedName)
        span?.setTag(TAG_MSG_PAYLOAD_TYPE, envelope.payload::class.qualifiedName)
        span?.setTag(TAG_CMD_EXPECT_VER, envelope.heading.expectedVersion)
        envelope.heading.sagaContext?.let {
            span?.setTag(TAG_MSG_SAGA_CTX, it)
        }
    }

    private fun opName(envelope: CommandEnvelope) =
        "${model.type.name}.handleCmd(${envelope.payload::class.java.simpleName})"

}
