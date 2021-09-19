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
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties

class ComplexSnapshotStore<S : EntityState, C : ComplexEntityState<S>> private constructor(
    private val rootStore: SnapshotStore<S>,
    private val stores: Map<KClass<*>, SnapshotStore.ChildEntityStore<*>>
) : SnapshotStore<C> {

    companion object {
        class Builder<S : EntityState, C : ComplexEntityState<S>> {
            lateinit var rootStore: SnapshotStore<S>
            val stores: MutableMap<KClass<*>, SnapshotStore.ChildEntityStore<*>> = mutableMapOf()

            fun build(): ComplexSnapshotStore<S, C> = ComplexSnapshotStore(rootStore, stores)
        }

        @Suppress("UNCHECKED_CAST")
        operator fun <R : EntityState, C : ComplexEntityState<R>> invoke(
            cls: KClass<C>,
            rootStore: SnapshotStore<R>,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<*>
        ): ComplexSnapshotStore<R, C> {
            val builder = Builder<R, C>()
            builder.rootStore = rootStore
            cls.memberProperties
                .filter { it.findAnnotation<ComplexEntityState.Children>() != null }
                .forEach { child ->
                    builder.process(child.returnType, storeProvider)
                }
            return builder.build()
        }

        private fun <S : EntityState, C : ComplexEntityState<S>> Builder<S, C>.processChildren(
            kcls: KClass<*>,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<*>
        ) {
            kcls.memberProperties
                .filter { it.findAnnotation<ComplexEntityState.Children>() != null }
                .forEach { child ->
                    process(child.returnType, storeProvider)
                }
        }

        private fun <S : EntityState, C : ComplexEntityState<S>> Builder<S, C>.process(
            kType: KType,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<*>
        ) {
            val kcls = kType.classifier as KClass<*>
            when {
                kcls == ImmutableList::class -> process(kType.arguments[0].type!!, storeProvider)
                kcls == ImmutableSet::class -> process(kType.arguments[0].type!!, storeProvider)
                kcls.isSubclassOf(ComplexEntityState::class) -> {
                    process(kcls.memberProperties.single { it.name == "root" }.returnType, storeProvider)
                    processChildren(kcls, storeProvider)
                }
                else -> {
                    stores[kcls] = storeProvider(kType)
                }
            }
        }
    }

    override suspend fun save(
        entityId: String,
        version: EventVersion,
        prevSnapshot: Snapshot<C>?,
        takeSnapshot: () -> Snapshot<C>
    ): Snapshot<C>? {
        TODO("Not yet implemented")
    }

    override suspend fun load(entityId: String): Snapshot<C>? {
        TODO("Not yet implemented")
    }

}