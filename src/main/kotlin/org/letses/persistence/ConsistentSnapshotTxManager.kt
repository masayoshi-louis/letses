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

package org.letses.persistence

import io.r2dbc.spi.R2dbcRollbackException
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionSynchronization
import org.springframework.transaction.reactive.TransactionSynchronizationManager
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono

interface ConsistentSnapshotTxManager {

    companion object {
        private val log = LoggerFactory.getLogger(ConsistentSnapshotTxManager::class.java)

        private val springTxDef = object : TransactionDefinition by TransactionDefinition.withDefaults() {
            override fun getPropagationBehavior() = TransactionDefinition.PROPAGATION_REQUIRES_NEW
        }

        fun spring(
            tm: ReactiveTransactionManager,
            txDef: TransactionDefinition = springTxDef
        ): ConsistentSnapshotTxManager = object : ConsistentSnapshotTxManager {
            @Suppress("UNCHECKED_CAST")
            override suspend fun <R> atomic(block: suspend () -> R): R {
                return atomic(null, block)
            }

            override fun afterCommit(action: suspend () -> Unit): AfterCompleteStep = object : AfterCompleteStep {
                override suspend fun <R> atomic(block: suspend () -> R): R {
                    val preAction = mono {
                        val tsm = TransactionSynchronizationManager.forCurrentTransaction().awaitSingle()
                        tsm.registerSynchronization(object : TransactionSynchronization {
                            override fun afterCommit(): Mono<Void> = mono {
                                action()
                                null
                            }
                        })
                    }
                    return atomic(preAction, block)
                }

            }

            @Suppress("UNCHECKED_CAST")
            private suspend fun <R> atomic(preAction: Mono<Unit>?, block: suspend () -> R): R {
                val tx = TransactionalOperator.create(tm, txDef)
                return mono {
                    preAction?.awaitSingleOrNull()
                    block()
                }.`as`(tx::transactional).awaitSingleOrNull() as R
            }
        }

        fun ConsistentSnapshotTxManager.withAutoRetry(errCode: Int = 40001): ConsistentSnapshotTxManager =
            object : ConsistentSnapshotTxManager by this {
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

        override fun afterCommit(action: suspend () -> Unit): AfterCompleteStep = object : AfterCompleteStep {
            override suspend fun <R> atomic(block: suspend () -> R): R {
                return this@PSEUDO.atomic(block).also {
                    action()
                }
            }
        }
    }

    suspend fun <R> atomic(block: suspend () -> R): R

    fun afterCommit(action: suspend () -> Unit): AfterCompleteStep

    interface AfterCompleteStep {
        suspend fun <R> atomic(block: suspend () -> R): R
    }

}
