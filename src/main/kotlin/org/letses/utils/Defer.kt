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

package org.letses.utils

import kotlinx.coroutines.*

typealias Defer = DeferScope

suspend fun <R> deferScope(block: suspend DeferScope.() -> R): R = coroutineScope {
    DeferScope(this).run(block)
}

suspend fun <R> CoroutineScope.deferScope(block: suspend DeferScope.() -> R): R = DeferScope(this).run(block)

class DeferScope(cs: CoroutineScope) : CoroutineScope by cs {
    companion object {
        suspend fun withAutoClose(vararg closeable: AutoCloseable, block: suspend DeferScope.() -> Unit) = deferScope {
            closeable.forEach {
                defer {
                    withContext(Dispatchers.IO) {
                        it.close()
                    }
                }
            }

            block()
        }
    }

    private val list = ArrayList<suspend () -> Unit>(3)

    fun defer(op: suspend () -> Unit) {
        list.add(op)
    }

    private suspend fun doDeferred(i: Int = list.size - 1) {
        if (i < 0) {
            return
        }
        try {
            list[i].invoke()
        } finally {
            doDeferred(i - 1)
        }
    }

    internal suspend fun <R> run(block: suspend DeferScope.() -> R): R {
        try {
            return block()
        } finally {
            withContext(NonCancellable) {
                doDeferred()
            }
        }
    }
}

fun CoroutineScope.launchWithDefer(block: suspend DeferScope.() -> Unit): Job = launch {
    deferScope {
        block()
    }
}


fun <T> CoroutineScope.asyncWithDefer(block: suspend DeferScope.() -> T): Deferred<T> = async {
    deferScope {
        block()
    }
}
