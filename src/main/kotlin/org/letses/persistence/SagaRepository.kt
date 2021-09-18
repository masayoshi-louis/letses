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

import org.letses.entity.EntityStateMachine
import org.letses.persistence.publisher.EventPublisher
import org.letses.saga.SagaDef
import org.letses.saga.SagaEvent
import org.letses.saga.SagaMsgHandlerContext
import org.letses.saga.Trigger

class SagaRepository<S : EntityStateMachine<S, T>, T : Trigger>(
    sagaDef: SagaDef<S, T>,
    eventStore: EventStore<SagaEvent<T>>,
    eventPublisher: EventPublisher<SagaEvent<T>>? = null
) : AbstractRepository<S, SagaEvent<T>, SagaMsgHandlerContext<T>>(sagaDef, eventStore, eventPublisher = eventPublisher)
