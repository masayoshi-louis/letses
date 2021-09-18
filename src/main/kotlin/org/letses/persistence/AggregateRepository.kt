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

import org.letses.domain.AggregateMsgHandlerContext
import org.letses.domain.EventSourcedAggregate
import org.letses.entity.EntityState
import org.letses.messaging.Event
import org.letses.persistence.publisher.EventPublisher

class AggregateRepository<S : EntityState, E : Event>(
    model: EventSourcedAggregate<S, E>,
    eventStore: EventStore<E>,
    snapshotStore: SnapshotStore<S>? = null,
    eventPublisher: EventPublisher<E>? = null,
    consistentSnapshotTxManager: ConsistentSnapshotTxManager = ConsistentSnapshotTxManager.PSEUDO,
) : AbstractRepository<S, E, AggregateMsgHandlerContext>(
    model,
    eventStore,
    snapshotStore,
    eventPublisher,
    consistentSnapshotTxManager
)
