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

package org.letses.utils.optics

import arrow.core.Option
import arrow.core.foldLeft
import arrow.core.foldMap
import arrow.optics.Every
import arrow.optics.Lens
import arrow.optics.Optional
import arrow.optics.PEvery
import arrow.optics.typeclasses.At
import arrow.optics.typeclasses.Index
import arrow.typeclasses.Monoid
import kotlinx.collections.immutable.*
import kotlinx.collections.immutable.adapters.ImmutableMapAdapter

fun <S, A> Lens<S, ImmutableList<A>>.at(
    i: Int,
    defaultSupplier: (Int) -> A = { throw IndexOutOfBoundsException() }
): Lens<S, A> =
    this compose listFocusAt(i, defaultSupplier)

fun <S, K, V> Lens<S, ImmutableMap<K, V>>.at(
    k: K,
    defaultSupplier: (K) -> V = { throw NoSuchElementException() }
): Lens<S, V> =
    this compose mapFocusAt(k, defaultSupplier)

fun <A> listFocusAt(defaultSupplier: (Int) -> A): (Int) -> Lens<ImmutableList<A>, A> = { i ->
    listFocusAt(i, defaultSupplier)
}

fun <A> listFocusAt(
    i: Int,
    defaultSupplier: (Int) -> A = { throw IndexOutOfBoundsException() }
): Lens<ImmutableList<A>, A> = Lens(
    get = { it.getOrNull(i) ?: defaultSupplier(i) },
    set = { list, v ->
        when {
            list.size > i -> list.toPersistentList().set(i, v)
            list.size == i -> list.toPersistentList().add(i, v)
            else -> throw IndexOutOfBoundsException()
        }
    }
)

fun <K, V> mapFocusAt(defaultSupplier: (K) -> V): (K) -> Lens<ImmutableMap<K, V>, V> = { k ->
    mapFocusAt(k, defaultSupplier)
}

fun <K, V> mapFocusAt(
    k: K,
    defaultSupplier: (K) -> V = { throw NoSuchElementException() }
): Lens<ImmutableMap<K, V>, V> = Lens(
    get = { it[k] ?: defaultSupplier(k) },
    set = { map, v -> map.toPersistentMap().put(k, v) }
)

fun <S, A> Optional<S, A>.withDefault(defaultSupplier: () -> A = { throw NullPointerException() }): Lens<S, A> = Lens(
    get = { this.getOrNull(it) ?: defaultSupplier() },
    set = { opt, v -> this.set(opt, v) }
)

fun <A> Index.Companion.immutableList(): Index<ImmutableList<A>, Int, A> = Index { i ->
    Optional(
        getOption = { list -> Option.fromNullable(list.getOrNull(i)) },
        set = { list, v ->
            when {
                list.size > i -> list.toPersistentList().set(i, v)
                list.size == i -> list.toPersistentList().add(i, v)
                else -> throw IndexOutOfBoundsException()
            }
        }
    )
}

fun <K, V> At.Companion.immutableMap(): At<ImmutableMap<K, V>, K, Option<V>> = At { i ->
    Lens(
        get = { Option.fromNullable(it[i]) },
        set = { map, optV ->
            optV.fold({
                map.toPersistentMap().remove(i)
            }, {
                map.toPersistentMap().put(i, it)
            })
        }
    )
}

fun <A> PEvery.Companion.immutableList(): Every<ImmutableList<A>, A> =
    object : Every<ImmutableList<A>, A> {
        override fun modify(source: ImmutableList<A>, map: (focus: A) -> A): ImmutableList<A> =
            source.map(map).toImmutableList()

        override fun <R> foldMap(M: Monoid<R>, source: ImmutableList<A>, map: (focus: A) -> R): R =
            source.foldMap(M, map)
    }

fun <K, V> PEvery.Companion.immutableMap(): Every<ImmutableMap<K, V>, V> =
    object : Every<ImmutableMap<K, V>, V> {
        override fun modify(source: ImmutableMap<K, V>, map: (focus: V) -> V): ImmutableMap<K, V> =
            ImmutableMapAdapter(source.mapValues { (_, v) -> map(v) })

        override fun <R> foldMap(M: Monoid<R>, source: ImmutableMap<K, V>, map: (focus: V) -> R): R =
            M.run {
                source.foldLeft(empty()) { acc, (_, v) ->
                    acc.combine(map(v))
                }
            }
    }
