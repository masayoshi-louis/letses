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
    private val stores: Map<KClass<*>, SnapshotStore<*>>
) : SnapshotStore<C> {

    companion object {
        class Builder<S : EntityState, C : ComplexEntityState<S>> {
            lateinit var rootStore: SnapshotStore<S>
            val stores: MutableMap<KClass<*>, SnapshotStore<*>> = mutableMapOf()

            fun build(): ComplexSnapshotStore<S, C> = ComplexSnapshotStore(rootStore, stores)
        }

        @Suppress("UNCHECKED_CAST")
        operator fun <R : EntityState, C : ComplexEntityState<R>> invoke(
            cls: KClass<C>,
            storeProvider: (KType) -> SnapshotStore<*>
        ): ComplexSnapshotStore<R, C> {
            val builder = Builder<R, C>()

            fun process(kType: KType) {
                val kcls = kType.classifier as KClass<*>
                when {
                    kcls == ImmutableList::class -> process(kType.arguments[0].type!!)
                    kcls == ImmutableSet::class -> process(kType.arguments[0].type!!)
                    kcls.isSubclassOf(ComplexEntityState::class) -> {
                        builder.stores[kcls] = invoke(
                            kcls as KClass<ComplexEntityState<EntityState>>,
                            storeProvider
                        )
                    }
                    else -> {
                        builder.stores[kcls] = storeProvider(kType)
                    }
                }
            }

            builder.rootStore =
                storeProvider(cls.memberProperties.single { it.name == "root" }.returnType) as SnapshotStore<R>
            cls.memberProperties
                .filter { it.findAnnotation<ComplexEntityState.Children>() != null }
                .forEach { child ->
                    process(child.returnType)
                }
            return builder.build()
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