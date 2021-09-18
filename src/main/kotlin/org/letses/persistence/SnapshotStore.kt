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

import kotlinx.coroutines.flow.Flow
import org.letses.entity.EntityState
import org.letses.eventsourcing.EventVersion

interface SnapshotStore<S : EntityState> {

    /**
     * @return saved snapshot, or null if no action has been taken
     */
    suspend fun save(
        entityId: String,
        version: EventVersion,
        prevSnapshot: Snapshot<S>?,
        takeSnapshot: () -> Snapshot<S>
    ): Snapshot<S>?

    suspend fun load(entityId: String): Snapshot<S>?

    interface ChildEntityStore<E : EntityState> {

        suspend fun save(state: E, parentId: String?)

        fun loadBy(parentId: String): Flow<E>

        suspend fun delete(entity: E, parentId: String?)

        suspend fun deleteAllBy(parentId: String)

    }

}