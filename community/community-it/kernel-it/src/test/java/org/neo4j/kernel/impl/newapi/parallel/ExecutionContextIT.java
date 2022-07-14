/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.newapi.parallel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.ExecutionContext;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;
import org.neo4j.util.concurrent.Futures;

@DbmsExtension
public class ExecutionContextIT {
    private static final int NUMBER_OF_WORKERS = 20;

    @Inject
    private GraphDatabaseAPI databaseAPI;

    private ExecutorService executors;

    @BeforeEach
    void setUp() {
        executors = Executors.newFixedThreadPool(NUMBER_OF_WORKERS);
    }

    @AfterEach
    void tearDown() {
        executors.shutdown();
    }

    @RepeatedTest(10)
    void contextMemoryTracking() throws ExecutionException {
        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = (KernelTransactionImplementation) ((InternalTransaction) transaction).kernelTransaction();
            var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
            var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
            for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                var executionContext = ktx.createExecutionContext();
                futures.add(executors.submit(() -> {
                    for (int j = 0; j < 5; j++) {
                        executionContext.memoryTracker().allocateHeap(10);
                    }
                    executionContext.complete();
                }));
                contexts.add(executionContext);
            }
            Futures.getAll(futures);

            KernelTransactions kernelTransactions =
                    databaseAPI.getDependencyResolver().resolveDependency(KernelTransactions.class);

            var transactionHandle = kernelTransactions.activeTransactions().stream()
                    .filter(tx -> tx.isUnderlyingTransaction(ktx))
                    .findFirst()
                    .orElseThrow();
            assertEquals(mebiBytes(40), transactionHandle.transactionStatistic().getEstimatedUsedHeapMemory());
            assertEquals(0, transactionHandle.transactionStatistic().getNativeAllocatedBytes());

            closeAllUnchecked(contexts);

            assertEquals(mebiBytes(40), transactionHandle.transactionStatistic().getEstimatedUsedHeapMemory());
            assertEquals(0, transactionHandle.transactionStatistic().getNativeAllocatedBytes());

            transaction.close();

            var statistic = new TransactionExecutionStatistic(ktx, Clocks.nanoClock(), 0);
            assertEquals(0, statistic.getEstimatedUsedHeapMemory());
            assertEquals(0, statistic.getNativeAllocatedBytes());
        }
    }

    @RepeatedTest(10)
    void contextAccessNodeExist() throws ExecutionException {
        int numberOfNodes = 1024;
        long[] nodeIds = new long[numberOfNodes];
        try (var transaction = databaseAPI.beginTx()) {
            for (int i = 0; i < numberOfNodes; i++) {
                Node node = transaction.createNode();
                nodeIds[i] = node.getId();
            }
            transaction.commit();
        }

        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
            var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
            for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                var executionContext = ktx.createExecutionContext();
                futures.add(executors.submit(() -> {
                    for (long nodeId : nodeIds) {
                        assertTrue(executionContext.dataRead().nodeExists(nodeId));
                    }
                    executionContext.complete();
                }));
                contexts.add(executionContext);
            }
            Futures.getAll(futures);
            closeAllUnchecked(contexts);
        }
    }

    @RepeatedTest(10)
    void contextAccessRelationshipExist() throws ExecutionException {
        int numberOfRelationships = 1024;
        long[] relIds = new long[numberOfRelationships];
        try (var transaction = databaseAPI.beginTx()) {
            for (int i = 0; i < numberOfRelationships; i++) {
                Node start = transaction.createNode();
                Node end = transaction.createNode();
                var relationship = start.createRelationshipTo(end, RelationshipType.withName("maker"));
                relIds[i] = relationship.getId();
            }
            transaction.commit();
        }

        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
            var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
            for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                var executionContext = ktx.createExecutionContext();
                futures.add(executors.submit(() -> {
                    for (long relId : relIds) {
                        assertTrue(executionContext.dataRead().relationshipExists(relId));
                    }
                    executionContext.complete();
                }));
                contexts.add(executionContext);
            }
            Futures.getAll(futures);
            closeAllUnchecked(contexts);
        }
    }

    @RepeatedTest(10)
    void contextPeriodicReport() throws ExecutionException {
        int numberOfNodes = 32768;
        long[] nodeIds = new long[numberOfNodes];
        try (var transaction = databaseAPI.beginTx()) {
            for (int i = 0; i < numberOfNodes; i++) {
                Node node = transaction.createNode();
                nodeIds[i] = node.getId();
            }
            transaction.commit();
        }
        int nodeSize = databaseAPI.databaseLayout() instanceof RecordDatabaseLayout
                ? NodeRecordFormat.RECORD_SIZE
                : 128; // 128B per node in freki
        int nodesPerPage = PageCache.PAGE_SIZE / nodeSize;
        int numPages = (int) Math.ceil((double) numberOfNodes / nodesPerPage);
        int numPins = numPages * NUMBER_OF_WORKERS;

        try (Transaction transaction = databaseAPI.beginTx()) {
            var ktx = ((InternalTransaction) transaction).kernelTransaction();
            var futures = new ArrayList<Future<?>>(NUMBER_OF_WORKERS);
            var contexts = new ArrayList<ExecutionContext>(NUMBER_OF_WORKERS);
            for (int i = 0; i < NUMBER_OF_WORKERS; i++) {
                var executionContext = ktx.createExecutionContext();
                futures.add(executors.submit(() -> {
                    for (long nodeId : nodeIds) {
                        assertTrue(executionContext.dataRead().nodeExists(nodeId));
                        if (nodeId % 100 == 0) {
                            executionContext.report();
                        }
                    }
                    executionContext.complete();
                }));
                contexts.add(executionContext);
            }
            Futures.getAll(futures);
            closeAllUnchecked(contexts);

            var tracer = ktx.cursorContext().getCursorTracer();
            assertEquals(numPins, tracer.pins());
            assertEquals(numPins, tracer.unpins());
            assertEquals(numPins, tracer.hits());
        }
    }

    @Test
    void closingExecutionContextDoNotLeakCursors() {
        for (int i = 0; i < 1024; i++) {
            try (Transaction transaction = databaseAPI.beginTx()) {
                var ktx = ((InternalTransaction) transaction).kernelTransaction();
                try (var executionContext = ktx.createExecutionContext()) {
                    executionContext.complete();
                }
            }
        }
    }
}
