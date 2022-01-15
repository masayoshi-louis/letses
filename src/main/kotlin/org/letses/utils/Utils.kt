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

package org.letses.utils

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.Consumer
import org.letses.command.BasicCommandEnvelope
import org.letses.command.NotInitializedException
import org.letses.command.commandHeading
import org.letses.domain.AggregateType
import org.letses.domain.DeleteCommand
import org.letses.platform.Platform
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.reflect.KParameter
import kotlin.reflect.full.functions

fun newUUID(): String = UUID.randomUUID().toString()

fun String.parseToUUID(): UUID = UUID.fromString(this)

@Deprecated("use Defer instead", replaceWith = ReplaceWith("\"org.letses.utils.Defer"))
suspend inline fun <T> Consumer<T>.use(block: Consumer<T>.() -> Unit) {
    try {
        block()
    } finally {
        withContext(NonCancellable) {
            closeAsync().await()
        }
    }
}

suspend inline fun <T> CompletableFuture<T>.awaitNoCancel(): T =
    withContext(NonCancellable) {
        await()
    }

suspend fun <A> Platform.deleteById(aggregate: A, id: String)
        where A : AggregateType<*, *>, A : DeleteCommand = coroutineScope {
    try {
        commandHandlerFor(aggregate).handle(
            BasicCommandEnvelope(
                commandHeading(targetId = id),
                aggregate.deleteCommand()
            )
        )
    } catch (e: NotInitializedException) {
        // silent
    }
}

@Suppress("UNCHECKED_CAST")
fun <T : Any> T.copy(values: Map<String, Any?>): T {
    val cls = this::class
    require(cls.isData)
    val copyFn = cls.functions.single { it.name == "copy" }
    val args = copyFn.parameters.mapNotNull {
        when {
            it.kind == KParameter.Kind.INSTANCE -> it to this
            it.name in values -> it to values[it.name]
            else -> null
        }
    }
    return copyFn.callBy(args.toMap()) as T
}
