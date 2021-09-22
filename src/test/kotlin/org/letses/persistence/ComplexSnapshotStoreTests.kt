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

@file:Suppress("RedundantNullableReturnType")

package org.letses.persistence

import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableSet
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.persistentSetOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.letses.entity.ComplexEntityState
import org.letses.entity.EntityState
import org.letses.eventsourcing.EventVersion
import org.letses.eventsourcing.NotExists
import org.mockito.Mockito
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.assertEquals


@Suppress("UNCHECKED_CAST")
class ComplexSnapshotStoreTests {

    data class R(val id: String, val r: Int) : EntityState() {
        override val identity: String = id
    }

    data class ChildAR(val ar: String) : EntityState() {
        override val identity: String = ar
    }

    data class ChildB(val b: Double) : EntityState() {
        override val identity: String = b.toString()
    }

    data class ChildA(
        override val root: ChildAR,
        @ComplexEntityState.Children
        val childAB: ChildB?
    ) : ComplexEntityState<ChildAR>()

    data class C(
        override val root: R,
        @ComplexEntityState.Children
        val childA: ImmutableList<ChildA>,
        @ComplexEntityState.Children
        val childB: ImmutableSet<ChildB>
    ) : ComplexEntityState<R>()

    interface TestSnapshotStore<S : EntityState> : SnapshotStore<S> {
        val kcls: KClass<S>
    }

    interface TestStore<S : EntityState> : SnapshotStore.ChildEntityStore<S> {
        val kcls: KClass<S>
    }

    @Test
    fun testBuilder() {
        val rStore = Mockito.mock(TestSnapshotStore::class.java) as TestSnapshotStore<R>
        val cStore = ComplexSnapshotStore(C::class, rStore) { ktype ->
            Mockito.mock(TestStore::class.java).also {
                Mockito.`when`(it.kcls).thenReturn(ktype.classifier as KClass<out EntityState>)
            } as TestStore<EntityState>
        }

        val cStoreStores = cStore.stores
        assertEquals(3, cStoreStores.size)
        assertEquals(ChildB::class, (cStoreStores[ChildB::class.createType()] as TestStore<*>).kcls)
        assertEquals(ChildB::class, (cStoreStores[ChildB::class.createType(nullable = true)] as TestStore<*>).kcls)
        assertEquals(ChildAR::class, (cStoreStores[ChildAR::class.createType()] as TestStore<*>).kcls)
    }

    @Test
    fun testSaveAndLoad() {
        val rStore = object : SnapshotStore<R> {
            val map = mutableMapOf<String, Snapshot<R>>()

            override suspend fun save(
                entityId: String,
                version: EventVersion,
                prevSnapshot: Snapshot<R>?,
                takeSnapshot: () -> Snapshot<R>
            ): Snapshot<R>? {
                return takeSnapshot().also {
                    map[entityId] = it
                }
            }

            override suspend fun load(entityId: String): Snapshot<R>? {
                return map[entityId]
            }
        }

        val childStore = mutableMapOf<Pair<KType, String>, List<EntityState>>()
        val cStore = ComplexSnapshotStore(C::class, rStore) { ktype ->
            object : SnapshotStore.ChildEntityStore<EntityState> {
                override suspend fun save(parentId: String, state: EntityState) {
                    childStore[ktype to parentId] = listOf(state)
                }

                override suspend fun saveAll(parentId: String, collection: Collection<EntityState>) {
                    childStore[ktype to parentId] = collection.toList()
                }

                override fun loadBy(parentId: String): Flow<EntityState> {
                    val list: List<EntityState> = childStore[ktype to parentId] ?: emptyList()
                    return list.asFlow()
                }

                override suspend fun deleteAllBy(parentId: String) {
                    childStore.remove(ktype to parentId)
                }
            }
        }

        val obj = C(
            root = R("id", 1),
            childA = persistentListOf(
                ChildA(ChildAR("ar1"), ChildB(1.0)),
                ChildA(ChildAR("ar2"), null)
            ),
            childB = persistentSetOf(ChildB(0.0))
        )

        runBlocking {
            cStore.save(obj.identity, NotExists, null) { BasicSnapshot(obj, 0, emptyList()) }
        }

        println(rStore.map)
        println(childStore)
    }

    private val ComplexSnapshotStore<*, *>.rootStore: SnapshotStore<*>
        get() = this::class.memberProperties
            .single { it.name == "rootStore" }.getter.run {
                isAccessible = true
                call(this@rootStore)
            } as SnapshotStore<*>

    private val ComplexSnapshotStore<*, *>.stores: Map<KType, SnapshotStore.ChildEntityStore<*>>
        get() = this::class.memberProperties
            .single { it.name == "stores" }.getter.run {
                isAccessible = true
                call(this@stores)
            } as Map<KType, SnapshotStore.ChildEntityStore<*>>
}