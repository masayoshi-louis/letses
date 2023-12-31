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

@file:Suppress("UNCHECKED_CAST")

package org.letses.persistence

import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.persistentMapOf
import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.flow.*
import org.letses.entity.ComplexEntityState
import org.letses.entity.DeleteFlag
import org.letses.entity.EntityState
import org.letses.eventsourcing.EventVersion
import org.letses.utils.copy
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.*

private typealias CPLX = ComplexEntityState<EntityState>

class ComplexSnapshotStore<S : EntityState, C : ComplexEntityState<S>> private constructor(
    private val rootStore: SnapshotStore<S>,
    private val stores: Map<KType, SnapshotStore.ChildEntityStore<EntityState>>,
    private val kType: KType
) : SnapshotStore<C> {

    companion object {

        class Builder<S : EntityState, C : ComplexEntityState<S>> {
            lateinit var rootStore: SnapshotStore<S>
            lateinit var kType: KType
            val stores: MutableMap<KType, SnapshotStore.ChildEntityStore<EntityState>> = mutableMapOf()

            fun build(): ComplexSnapshotStore<S, C> = ComplexSnapshotStore(rootStore, stores, kType)
        }

        operator fun <R : EntityState, C : ComplexEntityState<R>> invoke(
            cls: KClass<C>,
            rootStore: SnapshotStore<R>,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<out EntityState>
        ): ComplexSnapshotStore<R, C> {
            val builder = Builder<R, C>()
            builder.rootStore = rootStore
            builder.kType = cls.createType()
            cls.children.forEach {
                builder.process(it.returnType, storeProvider)
            }
            return builder.build()
        }

        private fun <S : EntityState, C : ComplexEntityState<S>> Builder<S, C>.processChildren(
            kType: KType,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<out EntityState>
        ) {
            kType.children.forEach {
                process(it.returnType, storeProvider)
            }
        }

        private fun <S : EntityState, C : ComplexEntityState<S>> Builder<S, C>.process(
            kType: KType,
            storeProvider: (KType) -> SnapshotStore.ChildEntityStore<out EntityState>
        ) {
            when {
                kType.isList -> {
                    require(!kType.isMarkedNullable)
                    val eKType = kType.arguments[0].type!!
                    require(!eKType.isMarkedNullable)
                    process(eKType, storeProvider)
                }
                kType.isMap -> {
                    require(!kType.isMarkedNullable)
                    val eKType = kType.arguments[1].type!!
                    require(!eKType.isMarkedNullable)
                    process(eKType, storeProvider)
                }
                kType.isComplex -> {
                    process(kType.rootType, storeProvider)
                    processChildren(kType, storeProvider)
                }
                else -> {
                    stores[kType] = storeProvider(kType) as SnapshotStore.ChildEntityStore<EntityState>
                }
            }
        }

        private val <T : Any> KClass<T>.isList: Boolean
            get() = this == ImmutableList::class

        private val <T : Any> KClass<T>.isMap: Boolean
            get() = this == ImmutableMap::class

        private val <T : Any> KClass<T>.isComplex: Boolean
            get() = isSubclassOf(ComplexEntityState::class)

        private val KType.isList: Boolean
            get() = (this.classifier as KClass<*>).isList

        private val KType.isMap: Boolean
            get() = (this.classifier as KClass<*>).isMap

        private val KType.isComplex: Boolean
            get() = (this.classifier as KClass<*>).isComplex

        private val SOFT_DELETE_TYPE = DeleteFlag.SoftDelete::class.createType()

        private val KType.isSoftDelete: Boolean
            get() = this.isSubtypeOf(SOFT_DELETE_TYPE)

        private val <T : ComplexEntityState<*>> KClass<out T>.children: List<KProperty1<out EntityState, Any?>>
            get() = memberProperties
                .filter { it.findAnnotation<ComplexEntityState.Children>() != null } as List<KProperty1<out EntityState, EntityState>>

        private val KType.children: List<KProperty1<out EntityState, Any?>>
            get() = (this.classifier as KClass<CPLX>).children

        private val KType.root: KProperty1<CPLX, *>
            get() = (this.classifier as KClass<CPLX>).root

        private val <T : ComplexEntityState<*>> KClass<T>.root: KProperty1<T, *>
            get() = memberProperties.single { it.name == "root" }

        private val KType.rootType: KType
            get() = root.returnType
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
        val state = snapshot.state as CPLX
        saveComplex(null, state::class.createType(), state, ctx)
        return snapshot
    }

    override suspend fun load(entityId: String): Snapshot<C>? {
        val rootSnapshot = rootStore.load(entityId) ?: return null
        val state = loadComplex(entityId, kType, rootSnapshot.state)
        return rootSnapshot.map { state as C }
    }

    @Suppress("NAME_SHADOWING")
    private suspend fun loadComplex(parentId: String, kType: KType, root: EntityState? = null): CPLX? {
        assert(kType.isComplex)
        val root = root ?: stores[kType.rootType]!!.loadBy(parentId).singleOrNull() ?: return null
        val cls = kType.classifier as KClass<CPLX>
        val constructor = cls.primaryConstructor!!
        val args = cls.children.map { prop ->
            val param = constructor.findParameterByName(prop.name)!!
            assert(param.kind == KParameter.Kind.VALUE)
            assert(param.type == prop.returnType)
            param to loadChild(root.identity, prop.returnType)
        }
        val rootParam = constructor.parameters[0]
        assert(rootParam.type == kType.rootType && rootParam.name == kType.root.name)
        return constructor.callBy(args.toMap() + (constructor.parameters[0] to root))
    }

    private suspend fun loadChild(parentId: String, kType: KType): Any? {
        return if (kType.isList) {
            loadList(parentId, kType.arguments[0].type!!).toList().toImmutableList()
        } else if (kType.isMap) {
            var result = persistentMapOf<Any, Any>()
            loadList(parentId, kType.arguments[1].type!!).collect {
                val k = if (it is ComplexEntityState.ChildEntity) {
                    it.localIdentity
                } else {
                    it.identity
                }
                result = result.put(k, it)
            }
            result
        } else if (kType.isComplex) {
            loadComplex(parentId, kType)
        } else {
            stores[kType]!!.loadBy(parentId).singleOrNull()
        }
    }

    private fun loadList(parentId: String, eKType: KType): Flow<EntityState> {
        return if (eKType.isComplex) {
            stores[eKType.rootType]!!.loadBy(parentId).map {
                loadComplex(parentId, eKType, it)!!
            }
        } else {
            stores[eKType]!!.loadBy(parentId)
        }
    }

    private suspend fun saveComplex(
        parentId: String?,
        kType: KType,
        current: CPLX?,
        ctx: Ctx
    ) {
        if (current == null) {
            return
        }

        val r = (current as ComplexEntityState<*>).root

        // parentId = null if the object is aggregate root
        if (parentId != null) {
            // not aggregate root, so persist the child root
            val rs = stores[kType.rootType]!!
            rs.save(r, parentId)
        }

        kType.children.forEach {
            ctx.pathPush(it)
            val obj = it.getter.call(current)
            if (obj != null && r is DeleteFlag && r.deleted) {
                delete(r.identity, it.returnType, obj, ctx)
            } else {
                saveChild(r.identity, it.returnType, obj, ctx)
            }
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
                delete(parentId, eKType, it, ctx)
                ctx.pathPop()
            }
        }
        if (current.isNotEmpty()) {
            current.forEach {
                val i = prevList.indexOfFirst { p -> p.identity == it.identity }
                if (i < 0) {
                    ctx.pathPushNull()
                } else {
                    ctx.pathPush(i)
                }
                if (i < 0 || prevList[i] != it) {
                    saveChild(parentId, eKType, it, ctx)
                }
                ctx.pathPop()
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
                delete(parentId, eKType, v, ctx)
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
        if (kType.isList) {
            require(current != null)
            saveList(parentId, kType.arguments[0].type!!, current as List<EntityState>, ctx)
        } else if (kType.isMap) {
            require(current != null)
            saveMap(parentId, kType.arguments[1].type!!, current as Map<Any, EntityState>, ctx)
        } else if (kType.isComplex) {
            saveComplex(parentId, kType, current as CPLX?, ctx)
        } else if (current != null) {
            stores[kType]!!.save(current as EntityState, parentId)
        } else {
            // current == null && prev != null
            ctx.prev()?.let { prev ->
                delete(parentId, kType, prev, ctx)
            }
        }
    }

    private suspend fun delete(parentId: String, kType: KType, obj: Any, ctx: Ctx) {
        if (kType.isComplex) {
            obj as CPLX
            kType.children.forEach { prop ->
                ctx.pathPush(prop)
                prop.getter.call(obj)?.let {
                    delete(obj.identity, prop.returnType, it, ctx)
                }
                ctx.pathPop()
            }
            ctx.pathPush(kType.root)
            delete(parentId, kType.rootType, obj.root, ctx)
            ctx.pathPop()
        } else if (kType.isList) {
            val eKType = kType.arguments[0].type!!
            if (eKType.isComplex || eKType.isSoftDelete) {
                (obj as List<Any>).forEachIndexed { i, it ->
                    ctx.pathPush(i)
                    delete(parentId, eKType, it, ctx)
                    ctx.pathPop()
                }
            } else {
                stores[eKType]!!.deleteAllBy(parentId)
            }
        } else if (kType.isMap) {
            val eKType = kType.arguments[1].type!!
            if (eKType.isComplex || eKType.isSoftDelete) {
                (obj as Map<Any, Any>).forEach { (k, v) ->
                    ctx.pathPush(k)
                    delete(parentId, eKType, v, ctx)
                    ctx.pathPop()
                }
            } else {
                stores[eKType]!!.deleteAllBy(parentId)
            }
        } else {
            if (obj is DeleteFlag.SoftDelete) {
                require((kType.classifier as KClass<*>).isData) { "only support data class" }
                val deletedObj = obj.copy(Collections.singletonMap(DeleteFlag::deleted.name, true))
                stores[kType]!!.save(deletedObj as EntityState, parentId)
            } else {
                stores[kType]!!.delete(obj as EntityState, parentId)
            }
        }
    }

    private inline fun <R, S> Snapshot<S>.map(f: (S) -> R): Snapshot<R> where R : EntityState, S : EntityState =
        BasicSnapshot(f(state), version, deduplicationMemory)

    private fun Snapshot<C>.asRoot(): Snapshot<S> = map { it.root }

    private class Ctx(val prevRootComplex: ComplexEntityState<*>?) {
        private sealed interface PathNode {
            fun get(parent: Any?): Any? = parent?.let { get0(it) }

            fun get0(parent: Any): Any?

            data class PropNode(val p: KProperty1<out EntityState, Any?>) : PathNode {
                override fun get0(parent: Any): Any? = p.call(parent)
            }

            data class IndexNode(val i: Int) : PathNode {
                override fun get0(parent: Any): Any? = (parent as List<Any?>)[i]
            }

            data class KeyNode(val k: Any) : PathNode {
                override fun get0(parent: Any): Any? = (parent as Map<Any, Any?>)[k]
            }

            object NullNode : PathNode {
                override fun get0(parent: Any): Any? = null
            }
        }

        private val path = ArrayList<PathNode>()

        fun prev() = path.fold(prevRootComplex as Any?) { acc, node ->
            node.get(acc)
        }

        fun pathPush(i: Int) {
            path.add(PathNode.IndexNode(i))
        }

        fun pathPush(k: Any) {
            path.add(PathNode.KeyNode(k))
        }

        fun pathPushNull() {
            path.add(PathNode.NullNode)
        }

        fun pathPush(p: KProperty1<out EntityState, *>) {
            path.add(PathNode.PropNode(p))
        }

        fun pathPop() {
            path.removeLast()
        }
    }

}
