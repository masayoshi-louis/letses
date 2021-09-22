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

@file:Suppress("UNCHECKED_CAST")

package org.letses.persistence

import arrow.core.Either
import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableMap
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
                kcls.isList -> {
                    require(!kType.isMarkedNullable)
                    val eKType = kType.arguments[0].type!!
                    require(!eKType.isMarkedNullable)
                    process(eKType, storeProvider)
                }
                kcls.isMap -> {
                    require(!kType.isMarkedNullable)
                    val eKType = kType.arguments[1].type!!
                    require(!eKType.isMarkedNullable)
                    process(eKType, storeProvider)
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

        private val <T : Any> KClass<T>.isList: Boolean
            get() = this == ImmutableList::class

        private val <T : Any> KClass<T>.isMap: Boolean
            get() = this == ImmutableMap::class

        private val <T : Any> KClass<T>.isComplex: Boolean
            get() = isSubclassOf(ComplexEntityState::class)

        private val KType.isComplex: Boolean
            get() = (this.classifier as KClass<*>).isComplex

        private val <T : Any> KClass<out T>.children: List<KProperty1<out EntityState, Any?>>
            get() = memberProperties
                .filter { it.findAnnotation<ComplexEntityState.Children>() != null } as List<KProperty1<out EntityState, EntityState>>

        private val <T : Any> KClass<T>.root: KProperty1<T, *>
            get() = memberProperties.single { it.name == "root" }
    } // end companion object

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
        val ctx = Ctx(prevSnapshot?.state)
        snapshot.state::class.children.forEach {
            ctx.pathPush(it)
            saveChild(entityId, it.returnType, it.getter.call(snapshot.state), ctx)
            ctx.pathPop()
        }
        return snapshot
    }

    override suspend fun load(entityId: String): Snapshot<C>? {
        TODO("Not yet implemented")
    }

    private suspend fun saveComplex(
        kType: KType,
        current: ComplexEntityState<EntityState>?,
        ctx: Ctx
    ) {
        if (current == null) {
            return
        }
        val cls = kType.classifier as KClass<*>
        val rs = stores[cls.root.returnType]!!
        val r = (current as ComplexEntityState<*>).root
        rs.save(r)
        cls.children.forEach {
            ctx.pathPush(it)
            saveChild(r.identity, it.returnType, it.getter.call(current), ctx)
            ctx.pathPop()
        }
    }

    private suspend fun saveList(
        parentId: String,
        eKType: KType,
        current: List<EntityState>,
        ctx: Ctx
    ) {
        val prevList = (ctx.prev() as List<EntityState>?) ?: emptyList()
        val currIdSet = current.map { e -> e.identity }.toSet()
        prevList.forEachIndexed { i, it ->
            if (it.identity !in currIdSet) {
                ctx.pathPush(i)
                delete(parentId, it, eKType, ctx)
                ctx.pathPop()
            }
        }
        if (current.isNotEmpty()) {
            val prevMap = prevList.groupBy { it.identity }
            current.forEachIndexed { i, it ->
                // skip unchanged
                if (prevMap[it.identity] != it) {
                    ctx.pathPush(i)
                    saveChild(parentId, eKType, it, ctx)
                    ctx.pathPop()
                }
            }
        }
    }

    private suspend fun saveMap(
        parentId: String,
        eKType: KType,
        current: Map<Any, EntityState>,
        ctx: Ctx
    ) {
        val prevMap = ctx.prev() as Map<Any, EntityState>? ?: emptyMap()
        prevMap.forEach { (k, v) ->
            if (k !in current) {
                ctx.pathPush(k)
                delete(parentId, v, eKType, ctx)
                ctx.pathPop()
            }
        }
        if (current.isNotEmpty()) {
            current.forEach { (k, v) ->
                // skip unchanged
                if (prevMap[k] != v) {
                    ctx.pathPush(k)
                    saveChild(parentId, eKType, v, ctx)
                    ctx.pathPop()
                }
            }
        }
    }

    private suspend fun saveChild(
        parentId: String,
        kType: KType,
        current: Any?,
        ctx: Ctx
    ) {
        val cls = kType.classifier as KClass<*>
        if (cls.isList) {
            require(current != null)
            saveList(parentId, kType.arguments[0].type!!, current as List<EntityState>, ctx)
        } else if (cls.isMap) {
            require(current != null)
            saveMap(parentId, kType.arguments[1].type!!, current as Map<Any, EntityState>, ctx)
        } else if (cls.isComplex) {
            saveComplex(kType, current as ComplexEntityState<EntityState>?, ctx)
        } else if (current != null) {
            stores[kType]!!.save(current as EntityState)
        } else {
            // current == null && prev != null
            ctx.prev()?.let { prev ->
                delete(parentId, prev, kType, ctx)
            }
        }
    }

    private suspend fun delete(parentId: String, obj: Any, kType: KType, ctx: Ctx) {
        val cls = kType.classifier as KClass<*>
        if (cls.isComplex) {
            obj as ComplexEntityState<EntityState>
            cls.children.forEach { prop ->
                ctx.pathPush(prop)
                prop.getter.call(obj)?.let {
                    delete(obj.identity, it, prop.returnType, ctx)
                }
                ctx.pathPop()
            }
            stores[cls.root.returnType]!!.delete(obj.root)
        } else if (cls.isList) {
            val eKType = kType.arguments[0].type!!
            if (eKType.isComplex) {
                (obj as List<ComplexEntityState<EntityState>>).forEachIndexed { i, it ->
                    ctx.pathPush(i)
                    delete(parentId, it, eKType, ctx)
                    ctx.pathPop()
                }
            } else {
                stores[eKType]!!.deleteAllBy(parentId)
            }
        } else if (cls.isMap) {
            val eKType = kType.arguments[1].type!!
            if (eKType.isComplex) {
                (obj as Map<Any, ComplexEntityState<EntityState>>).forEach { (k, v) ->
                    ctx.pathPush(k)
                    delete(parentId, v, eKType, ctx)
                    ctx.pathPop()
                }
            } else {
                stores[eKType]!!.deleteAllBy(parentId)
            }
        } else {
            stores[kType]!!.delete(obj as EntityState)
        }
    }

    private fun Snapshot<C>.asRoot(): Snapshot<S> = BasicSnapshot(state.root, version, deduplicationMemory)

    private class Ctx(val prevRootComplex: ComplexEntityState<*>?) {
        private val path = ArrayList<Either<KProperty1<out EntityState, Any?>, Either<Int, Any>>>()

        fun prev() = path.fold(prevRootComplex as Any?) { acc, node ->
            if (acc == null) {
                null
            } else {
                when (node) {
                    is Either.Left -> node.a.getter.call(acc)
                    is Either.Right -> {
                        when (val index = node.b) {
                            is Either.Left -> (acc as List<Any?>)[index.a]
                            is Either.Right -> (acc as Map<Any, Any?>)[index.b]
                        }
                    }
                }
            }
        }

        fun pathPush(i: Int) {
            path.add(Either.right(Either.left(i)))
        }

        fun pathPush(k: Any) {
            path.add(Either.right(Either.right(k)))
        }

        fun pathPush(p: KProperty1<out EntityState, *>) {
            path.add(Either.left(p))
        }

        fun pathPop() {
            path.removeLast()
        }
    }

}
