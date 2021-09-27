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

import arrow.core.identity
import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.persistentMapOf
import kotlinx.coroutines.coroutineScope
import org.letses.entity.ComplexEntityState
import org.letses.entity.EntityState
import org.letses.entity.StandardEntityState
import org.letses.eventsourcing.EventSourced
import org.letses.eventsourcing.EventVersion
import org.letses.eventsourcing.NotExists
import org.letses.eventsourcing.ProduceExtraEventHeaders
import org.letses.messaging.*
import org.letses.platform.MsgHandlerContext
import org.letses.utils.copy
import org.letses.utils.newUUID
import org.letses.utils.tracing.span
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty
import kotlin.reflect.full.createType
import kotlin.reflect.full.functions
import kotlin.reflect.full.isSupertypeOf


abstract class AbstractTransaction<S : EntityState, E : Event, in C : MsgHandlerContext>(
    private val model: EventSourced<S, E, C>,
    private val entityId: String,
    private val correlationId: String,
    private val msgIdExtractor: (Any) -> String,
    private val partitionKey: String? = null,
    private val deduplicationMemSize: Int = 0,
    private val enhanceHeading: (EventHeading) -> EventHeading = ::identity,
    private val deferStateEval: Int = 10,
    private val consistentSnapshotTxManager: ConsistentSnapshotTxManager = ConsistentSnapshotTxManager.PSEUDO,
) : Transaction<S, E, C> {
    companion object {
        private val EmptyImmutableMap = persistentMapOf<String, String>()

        private val log = LoggerFactory.getLogger(AbstractTransaction::class.java)

        private val copyFnCache = ConcurrentHashMap<KClass<*>, (EntityState, EventEnvelope<*>) -> EntityState>()

        private val uuidType = UUID::class.createType()

        private fun copyWithStandardFieldsUpdated(state: EntityState, ev: EventEnvelope<*>): EntityState {
            val copy = copyFnCache.computeIfAbsent(state::class) { cls ->
                val copy = cls.functions.single { it.name == "copy" }
                var pInst: KParameter? = null
                var pVer: KParameter? = null
                var pModified: KParameter? = null
                var pCreated: KParameter? = null
                var pId: KParameter? = null
                copy.parameters.forEach { p ->
                    when {
                        p.kind == KParameter.Kind.INSTANCE -> pInst = p
                        p.name == StandardEntityState<*>::version.name -> pVer = p
                        p.name == StandardEntityState<*>::modified.name -> pModified = p
                        p.name == StandardEntityState<*>::created.name -> pCreated = p
                        p.name == StandardEntityState<*>::id.name -> pId = p
                    }
                }
                // return a function
                return@computeIfAbsent { state, ev ->
                    val args = mutableMapOf<KParameter, Any?>()
                    args[pInst!!] = state
                    args[pVer!!] = ev.heading.version
                    args[pModified!!] = ev.heading.timestamp
                    if (ev.heading.version == 0L) {
                        args[pCreated!!] = ev.heading.timestamp
                        if (pId!!.type.isSupertypeOf(uuidType)) {
                            args[pId!!] = UUID.fromString(ev.heading.sourceId)
                        }
                    }
                    copy.callBy(args) as EntityState
                }
            } // end: computeIfAbsent
            return copy.invoke(state, ev)
        }
    }

    private val stateLazyEval = StateLazyEval()
    private var pendingEvents = persistentListOf<PersistentEventEnvelope<E>>()
    private var committedEvents: ImmutableList<PersistentEventEnvelope<E>>? = null

    override val state: S by stateLazyEval
    override var version: EventVersion = NotExists

    private val deduplicationMemory = Collections.newSetFromMap(object : LinkedHashMap<String, Boolean>() {
        override fun removeEldestEntry(eldest: Map.Entry<String, Boolean>?): Boolean {
            return size > deduplicationMemSize
        }
    })

    override val isEntityExists get() = version != NotExists

    override suspend fun commit(): List<PersistentEventEnvelope<E>> = coroutineScope {
        span?.run {
            log(mapOf("event" to "Repository.Transaction.committing", "numEvents" to pendingEvents.size))
        }
        val ret = pendingEvents
        if (pendingEvents.isNotEmpty()) {
            consistentSnapshotTxManager.atomic {
                saveEvents(pendingEvents, version - pendingEvents.size)
                saveSnapshot(::taskSnapshot)
            }
            committedEvents = pendingEvents
            pendingEvents = persistentListOf()
            log.debug("<$correlationId>[${model.eventCategory}_$entityId] committed, ${ret.size} event(s) saved")
            publishEvents(::lastCommittedEvents)
        } else {
            log.debug("<$correlationId>[${model.eventCategory}_$entityId] committed, no new event")
        }
        span?.log("Repository.Transaction.committed")
        ret
    }

    override fun lastCommittedEvents(): List<PersistentEventEnvelope<E>> = this.committedEvents ?: persistentListOf()

    override fun checkVersion(expected: EventVersion) {
        if (version != expected)
            throw ConcurrentModificationException("current version is $version while $expected is expected")
    }

    override fun handleMsg(msg: Any, ctx: C): EventEnvelope<E>? {
        val causalityId = msgIdExtractor(msg) // i.e. Command Id
        if (deduplicationMemory.contains(causalityId)) return null // ignore duplicated message
        val pair = when (model) {
            is ProduceExtraEventHeaders<S, E, C> -> model.handleMsgProduceExtraEventHeaders(state, msg, ctx)
            else -> model.handleMsg(state, msg, ctx)?.let { Pair(it, EmptyImmutableMap) }
        }
        if (pair != null) {
            val (e, eh) = pair
            val h = BasicEventHeading(
                sourceId = entityId,
                eventId = newUUID(),
                version = version + 1,
                causalityId = causalityId,
                correlationId = correlationId,
                partitionKey = partitionKey,
                extra = eh
            ).let(enhanceHeading).toBasic()
            val pe = PersistentEventEnvelope(h, e)
            applyEvent(pe)
            pendingEvents = pendingEvents.add(pe)
            // log
            ctx.span?.log("Repository.Transaction.eventsGenerated")
            if (log.isDebugEnabled) {
                val eCls = if (e is WrappedProtobufEvent) {
                    e.message::class.java
                } else {
                    e::class.java
                }
                log.debug("<$correlationId>[${model.eventCategory}_$entityId] generated event ${eCls.simpleName}(${h.eventId})")
            }
            return pe
        } else {
            memorizeMsgId(causalityId)
            return null
        }
    }

    protected fun loadSnapshot(s: Snapshot<S>) {
        stateLazyEval.value = s.state
        version = s.version
        deduplicationMemory.clear()
        deduplicationMemory.addAll(s.deduplicationMemory)
    }

    protected fun applyEvent(pe: PersistentEventEnvelope<E>) {
        stateLazyEval.add(pe)
        version = pe.heading.version
        memorizeMsgId(pe.heading.causalityId)
    }

    protected abstract suspend fun saveEvents(events: List<PersistentEventEnvelope<E>>, expectedVersion: EventVersion)

    protected abstract suspend fun saveSnapshot(takeSnapshot: () -> Snapshot<S>)

    protected abstract suspend fun publishEvents(events: () -> List<PersistentEventEnvelope<E>>)

    private fun taskSnapshot(): Snapshot<S> = BasicSnapshot(state, version, deduplicationMemory.toList())

    private fun memorizeMsgId(id: String) {
        if (deduplicationMemSize > 0) deduplicationMemory.add(id)
    }

    private inner class StateLazyEval {
        private var state: S = model.emptyState
        private var pendingEvents = mutableListOf<PersistentEventEnvelope<E>>()

        fun add(pe: PersistentEventEnvelope<E>) {
            if (pendingEvents.size >= deferStateEval) {
                value // force evaluate
            }
            pendingEvents.add(pe)
        }

        var value: S
            get() = if (pendingEvents.isEmpty()) state else {
                pendingEvents.fold(state, ::applyEvent).also {
                    state = it // cache
                    pendingEvents.clear()
                }
            }
            set(value) {
                state = value
                pendingEvents.clear()
            }

        operator fun getValue(thisRef: Any?, property: KProperty<*>) = value

        @Suppress("UNCHECKED_CAST")
        private fun applyEvent(state: S, ev: EventEnvelope<E>): S {
            assert(ev.heading.sourceId == entityId)
            val s = model.applyEvent(state, ev)
            if (s !is StandardEntityState<*> || !s::class.isData) {
                return s
            }
            return if (s is ComplexEntityState<*>) {
                val root = copyWithStandardFieldsUpdated(s.root, ev)
                s.copy(Collections.singletonMap(ComplexEntityState<*>::root.name, root))
            } else {
                copyWithStandardFieldsUpdated(s, ev) as S
            }
        }
    }

}
