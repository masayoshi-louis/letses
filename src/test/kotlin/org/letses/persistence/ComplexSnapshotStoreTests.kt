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

@file:Suppress("RedundantNullableReturnType")

package org.letses.persistence

import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.persistentListOf
import kotlinx.collections.immutable.persistentMapOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.filter
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
import kotlin.test.assertTrue


@Suppress("UNCHECKED_CAST")
class ComplexSnapshotStoreTests {

    data class R(val id: String, val r: Int) : EntityState() {
        override val identity: String = id
    }

    data class ChildAR(val ar: String) : EntityState() {
        override val identity: String = ar
    }

    data class ChildB(val id: String, val b: Double) : EntityState() {
        override val identity: String = id
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
        val childB: ImmutableMap<String, ChildB>
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

        // Mock stores
        val childStore = mutableMapOf<Pair<KType, String>, EntityState>()
        val childStoreByParent = mutableMapOf<Pair<KType, String>, List<EntityState>>()
        val cStore = ComplexSnapshotStore(C::class, rStore) { ktype ->
            object : SnapshotStore.ChildEntityStore<EntityState> {
                override suspend fun save(state: EntityState, parentId: String?) {
                    childStore[ktype to state.identity] = state
                    val list = ArrayList(childStoreByParent[ktype to parentId!!] ?: emptyList())
                    list.removeIf { it.identity == state.identity }
                    list.add(state)
                    childStoreByParent[ktype to parentId] = list
                }

                override fun loadBy(parentId: String): Flow<EntityState> {
                    val list: List<EntityState> = childStoreByParent[ktype to parentId] ?: emptyList()
                    return list.asFlow().filter {
                        (ktype to it.identity) in childStore
                    }
                }

                override suspend fun deleteAllBy(parentId: String) {
                    childStoreByParent[ktype to parentId]?.forEach {
                        childStore.remove(ktype to it.identity)
                    }
                    childStoreByParent.remove(ktype to parentId)
                }

                override suspend fun delete(entity: EntityState, parentId: String?) {
                    childStore.remove(entity::class.createType() to entity.identity)
                    childStore.remove(entity::class.createType(nullable = true) to entity.identity)
                }
            }
        }

        val obj = C(
            root = R("id", 1),
            childA = persistentListOf(
                ChildA(ChildAR("ar1"), ChildB("ar1.b", 1.0)),
                ChildA(ChildAR("ar2"), null)
            ),
            childB = persistentMapOf("c.b1" to ChildB("c.b1", 0.0))
        )

        runBlocking {
            val snapshot1 = cStore.save(obj.identity, NotExists, null) { BasicSnapshot(obj, 0, emptyList()) }

            assertTrue(childStore.isNotEmpty())
            assertTrue(childStoreByParent.isNotEmpty())

            val snapshot2 = cStore.load(obj.identity)!!
            assertEquals(snapshot1, snapshot2)

            val modified = C(
                root = R("id", 1),
                childA = persistentListOf(
                    ChildA(ChildAR("ar3"), null),
                    ChildA(ChildAR("ar2"), ChildB("ar2.b", 1.0))
                ),
                childB = persistentMapOf("c.b2" to ChildB("c.b2", 2.0))
            )

            val snapshot3 = cStore.save(obj.identity, snapshot2.version, snapshot2) {
                BasicSnapshot(
                    modified,
                    snapshot2.version + 1,
                    emptyList()
                )
            }

            val snapshot4 = cStore.load(obj.identity)!!
            assertEquals(snapshot3, snapshot4)
        }
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