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

package org.letses.utils.tracing

import io.github.shinigami.coroutineTracingApi.ActiveSpan
import io.github.shinigami.coroutineTracingApi.injectTracing
import io.github.shinigami.coroutineTracingApi.span
import io.github.shinigami.coroutineTracingApi.withTrace
import io.opentracing.Span
import io.opentracing.Tracer
import io.opentracing.util.GlobalTracer
import io.streamnative.pulsar.tracing.TracingPulsarUtils
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.reactor.mono
import org.apache.pulsar.client.api.Message
import org.letses.utils.Defer
import org.letses.utils.deferScope
import org.letses.utils.newUUID
import reactor.core.publisher.Mono
import kotlin.coroutines.coroutineContext

val CoroutineScope.span: Span?
    get() = coroutineContext[ActiveSpan]?.span

suspend inline fun <T> injectTracing(
    msg: Message<T>,
    tracingEnabled: Boolean = true,
    operationName: String = "handleMsg",
    crossinline handle: suspend CoroutineScope.(Message<T>) -> Unit
) {
    val spanContext = if (tracingEnabled) {
        TracingPulsarUtils.extractSpanContext(msg, GlobalTracer.get())
    } else {
        null
    }
    if (spanContext != null) {
        injectTracing(GlobalTracer.get(), {
            span(operationName) {
                asChildOf(spanContext)
            }
        }) {
            handle(msg)
        }
    } else {
        coroutineScope {
            handle(msg)
        }
    }
}

val CoroutineScope.traced: Boolean get() = coroutineContext[ActiveSpan] != null

fun CoroutineScope.traceIdOrRandom(): String = span?.context()?.toTraceId() ?: newUUID()

fun <R> tracedMono(block: suspend Defer.() -> R): Mono<R> {
    val tracer = GlobalTracer.get()
    val span = tracer.activeSpan()
    return mono {
        injectTracing(tracer, {
            span("handler") { asChildOf(span) }
        }) {
            deferScope {
                block()
            }
        }
    }
}

suspend inline fun <R> withTraceOpt(
    operationName: String,
    noinline builder: Tracer.SpanBuilder.() -> Tracer.SpanBuilder = { this },
    noinline cleanup: Span.(error: Throwable?) -> Unit = { this.finish() },
    crossinline block: suspend CoroutineScope.() -> R
): R {
    return if (coroutineContext[ActiveSpan] != null) {
        withTrace(operationName, builder, cleanup) {
            block()
        }
    } else {
        coroutineScope {
            block()
        }
    }
}
