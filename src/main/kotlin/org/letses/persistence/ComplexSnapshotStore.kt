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

import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableSet
import org.letses.entity.ComplexEntityState
import org.letses.entity.EntityState
import org.letses.eventsourcing.EventVersion
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties

class ComplexSnapshotStore<S : EntityState, C : ComplexEntityState<S>> private constructor(
    private val rootStore: SnapshotStore<S>,
    private val stores: Map<KType, SnapshotStore.ChildEntityStore<EntityState>>
) : SnapshotStore<C> {

    companion object {
        class Builder<S : EntityState, C : ComplexEntityState<S>> {
            lateinit var rootStore: SnapshotStore<S>
            val stores: MutableMap<KType, SnapshotStore.ChildEntityStore<EntityState>> = mutableMapOf()

            fun build(): ComplexSnapshotStore<S, C> = ComplexSnapshotStore(rootStore, stores)
        }

        @Suppress("UNCHECKED_CAST")
        operator fun <R : EntityState, C : ComplexEntityState<R>> invoke(
            cls: KClass<C>,
            rootStore: SnapshotStore<R>,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<EntityState>
        ): ComplexSnapshotStore<R, C> {
            val builder = Builder<R, C>()
            builder.rootStore = rootStore
            cls.children.forEach {
                builder.process(it.returnType, storeProvider)
            }
            return builder.build()
        }

        private fun <S : EntityState, C : ComplexEntityState<S>> Builder<S, C>.processChildren(
            kcls: KClass<*>,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<EntityState>
        ) {
            kcls.children.forEach {
                process(it.returnType, storeProvider)
            }
        }

        private fun <S : EntityState, C : ComplexEntityState<S>> Builder<S, C>.process(
            kType: KType,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<EntityState>
        ) {
            val kcls = kType.classifier as KClass<*>
            when {
                kcls.isCollection -> {
                    require(!kType.isMarkedNullable)
                    process(kType.arguments[0].type!!, storeProvider)
                }
                kcls.isComplex -> {
                    process(kcls.root.returnType, storeProvider)
                    processChildren(kcls, storeProvider)
                }
                else -> {
                    stores[kType] = storeProvider(kType)
                }
            }
        }

        private val <T : Any> KClass<T>.isCollection: Boolean
            get() = this == ImmutableList::class || this == ImmutableSet::class

        private val <T : Any> KClass<T>.isComplex: Boolean
            get() = isSubclassOf(ComplexEntityState::class)

        private val <T : Any> KClass<T>.children: List<KProperty1<T, *>>
            get() = memberProperties
                .filter { it.findAnnotation<ComplexEntityState.Children>() != null }

        private val <T : Any> KClass<T>.root: KProperty1<T, *>
            get() = memberProperties.single { it.name == "root" }
    }

    @Suppress("RedundantNullableReturnType")
    override suspend fun save(
        entityId: String,
        version: EventVersion,
        prevSnapshot: Snapshot<C>?,
        takeSnapshot: () -> Snapshot<C>
    ): Snapshot<C>? {
        val snapshot = takeSnapshot()
        rootStore.save(entityId, version, prevSnapshot?.asRoot()) {
            snapshot.asRoot()
        }
        snapshot::class.children.forEach {
            saveChild(entityId, it.returnType, it.getter.call(snapshot.state))
        }
        return snapshot
    }

    override suspend fun load(entityId: String): Snapshot<C>? {
        TODO("Not yet implemented")
    }

    private suspend fun saveChild(parentId: String, kType: KType, current: Any?) {
        val cls = kType.classifier as KClass<*>
        if (cls.isComplex) {
            val rs = stores[cls.root.returnType]!!
            val r = (current as ComplexEntityState<*>).root
            rs.save(parentId, r)
            cls.children.forEach {
                val child = it.getter.call(current)
                saveChild(r.identity, it.returnType, child)
            }
        } else {
            val s = if (current is Collection<*>) {
                assert(cls.isCollection)
                stores[kType.arguments[0].type]!!
            } else {
                stores[kType]!!
            }
            when (current) {
                null -> {
                    s.deleteAllBy(parentId)
                }
                is Collection<*> -> {
                    @Suppress("UNCHECKED_CAST")
                    s.saveAll(parentId, current as Collection<EntityState>)
                }
                else -> {
                    s.save(parentId, current as EntityState)
                }
            }
        }
    }

    private fun Snapshot<C>.asRoot(): Snapshot<S> = BasicSnapshot(state.root, version, deduplicationMemory)

}
