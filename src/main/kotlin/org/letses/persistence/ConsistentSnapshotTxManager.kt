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

import io.r2dbc.spi.R2dbcRollbackException
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionalOperator

interface ConsistentSnapshotTxManager {

    companion object {
        private val log = LoggerFactory.getLogger(ConsistentSnapshotTxManager::class.java)

        private val springTxDef = object : TransactionDefinition by TransactionDefinition.withDefaults() {
            override fun getPropagationBehavior() = TransactionDefinition.PROPAGATION_REQUIRES_NEW
        }

        fun spring(tm: ReactiveTransactionManager): ConsistentSnapshotTxManager =
            object : ConsistentSnapshotTxManager {
                @Suppress("UNCHECKED_CAST")
                override suspend fun <R> atomic(block: suspend () -> R): R {
                    val tx = TransactionalOperator.create(tm, springTxDef)
                    return mono {
                        block()
                    }.`as`(tx::transactional).awaitSingleOrNull() as R
                }
            }

        fun ConsistentSnapshotTxManager.withAutoRetry(errCode: Int = 40001): ConsistentSnapshotTxManager =
            object : ConsistentSnapshotTxManager {
                override suspend fun <R> atomic(block: suspend () -> R): R {
                    while (true) {
                        try {
                            return this@withAutoRetry.atomic(block)
                        } catch (e: R2dbcRollbackException) {
                            if (e.errorCode == errCode || e.sqlState == errCode.toString()) {
                                log.info("auto retry, exceptionMsg: ${e.message}")
                            } else {
                                log.error("transaction failed", e)
                                throw e
                            }
                        }
                    }
                }
            }
    }

    object PSEUDO : ConsistentSnapshotTxManager {
        override suspend fun <R> atomic(block: suspend () -> R): R {
            return block()
        }
    }

    suspend fun <R> atomic(block: suspend () -> R): R

}
