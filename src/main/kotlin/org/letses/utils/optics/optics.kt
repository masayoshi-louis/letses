package org.letses.utils.optics

import arrow.core.Option
import arrow.optics.Lens
import arrow.optics.Optional
import arrow.optics.typeclasses.At
import arrow.optics.typeclasses.Index
import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.toPersistentList
import kotlinx.collections.immutable.toPersistentMap

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
