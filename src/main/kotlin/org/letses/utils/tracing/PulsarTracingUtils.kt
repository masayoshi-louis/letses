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
import io.opentracing.util.GlobalTracer
import io.streamnative.pulsar.tracing.TypeMessageBuilderInjectAdapter
import kotlinx.coroutines.CoroutineScope
import org.apache.pulsar.client.api.TypedMessageBuilder

fun <T> CoroutineScope.injectTracingTo(message: TypedMessageBuilder<T>) {
    coroutineContext[ActiveSpan]?.span?.context()?.let { ctx ->
        GlobalTracer.get()
            .inject(
                ctx,
                io.opentracing.propagation.Format.Builtin.TEXT_MAP,
                TypeMessageBuilderInjectAdapter(message)
            );
    }
}
