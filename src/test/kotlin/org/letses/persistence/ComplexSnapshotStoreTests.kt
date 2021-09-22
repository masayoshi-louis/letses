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
import org.junit.Test
import org.letses.entity.ComplexEntityState
import org.letses.entity.EntityState
import org.mockito.Mockito
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.isAccessible
import kotlin.test.assertEquals


@Suppress("UNCHECKED_CAST")
class ComplexSnapshotStoreTests {

    data class R(val r: Int) : EntityState() {
        override val identity: String = r.toString()
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
        val childAB: ChildB
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
        assertEquals(2, cStoreStores.size)
        assertEquals(ChildB::class, (cStoreStores[ChildB::class.createType()] as TestStore<*>).kcls)
        assertEquals(ChildAR::class, (cStoreStores[ChildAR::class.createType()] as TestStore<*>).kcls)
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