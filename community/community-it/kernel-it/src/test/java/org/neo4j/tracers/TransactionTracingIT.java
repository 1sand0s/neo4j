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
package org.neo4j.tracers;

import static org.neo4j.graphdb.RelationshipType.withName;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@ExtendWith(SoftAssertionsExtension.class)
@DbmsExtension
class TransactionTracingIT {
    private static final int ENTITY_COUNT = 1_000;

    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private DatabaseManagementService managementService;

    @InjectSoftAssertions
    private SoftAssertions softly;

    @Test
    void tracePageCacheAccessOnAllNodesAccess() {
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < ENTITY_COUNT; i++) {
                transaction.createNode();
            }
            transaction.commit();
        }

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor(cursorContext);

            softly.assertThat(Iterables.count(transaction.getAllNodes()))
                    .as("Number of expected nodes")
                    .isEqualTo(ENTITY_COUNT);

            assertTraces(cursorContext, isRecordFormat() ? traces(2, 2, 2) : traces(16, 15, 16));
        }
    }

    @Test
    void tracePageCacheAccessOnNodeCreation() {
        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var cursorContext = transaction.kernelTransaction().cursorContext();

            var commitCursorChecker = new CommitCursorChecker(
                    cursorContext, isRecordFormat() ? traces(1001, 1001, 999, 2) : traces(2001, 2001, 1985, 16));
            managementService.registerTransactionEventListener(database.databaseName(), commitCursorChecker);

            for (int i = 0; i < ENTITY_COUNT; i++) {
                transaction.createNode();
            }
            assertZeroCursor(cursorContext);

            transaction.commit();
            softly.assertThat(commitCursorChecker.isInvoked())
                    .as("Transaction committed")
                    .isTrue();
        }
    }

    @Test
    void tracePageCacheAccessOnAllRelationshipsAccess() {
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < ENTITY_COUNT; i++) {
                var source = transaction.createNode();
                source.createRelationshipTo(transaction.createNode(), withName("connection"));
            }
            transaction.commit();
        }

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor(cursorContext);

            softly.assertThat(Iterables.count(transaction.getAllRelationships()))
                    .as("Number of expected relationships")
                    .isEqualTo(ENTITY_COUNT);

            assertTraces(cursorContext, isRecordFormat() ? traces(5, 5, 5) : traces(32, 31, 32));
        }
    }

    @Test
    void tracePageCacheAccessOnFindNodes() {
        var marker = Label.label("marker");
        var type = withName("connection");
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < ENTITY_COUNT; i++) {
                var source = transaction.createNode(marker);
                source.createRelationshipTo(transaction.createNode(), type);
            }
            transaction.commit();
        }

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor(cursorContext);

            softly.assertThat(Iterators.count(transaction.findNodes(marker)))
                    .as("Number of expected nodes")
                    .isEqualTo(ENTITY_COUNT);

            softly.assertThat(cursorContext.getCursorTracer().pins())
                    .as("Number of cursor pins")
                    .isEqualTo(1);
            softly.assertThat(cursorContext.getCursorTracer().unpins())
                    .as("Number of cursor unpins")
                    .isEqualTo(1);
            softly.assertThat(cursorContext.getCursorTracer().hits())
                    .as("Number of cursor hits")
                    .isEqualTo(1);
        }
    }

    @Test
    void tracePageCacheAccessOnFindRelationships() {
        var marker = Label.label("marker");
        var type = withName("connection");
        try (Transaction transaction = database.beginTx()) {
            for (int i = 0; i < ENTITY_COUNT; i++) {
                var source = transaction.createNode(marker);
                source.createRelationshipTo(transaction.createNode(), type);
            }
            transaction.commit();
        }

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor(cursorContext);

            softly.assertThat(Iterators.count(transaction.findRelationships(type)))
                    .as("Number of expected relationships")
                    .isEqualTo(ENTITY_COUNT);

            assertTraces(cursorContext, isRecordFormat() ? traces(1, 1, 1) : traces(33, 32, 33));
        }
    }

    @Test
    void tracePageCacheAccessOnDetachDelete() throws KernelException {
        var type = withName("connection");
        long sourceId;
        try (Transaction transaction = database.beginTx()) {
            var source = transaction.createNode();
            for (int i = 0; i < 10; i++) {
                source.createRelationshipTo(transaction.createNode(), type);
            }
            sourceId = source.getId();
            transaction.commit();
        }

        try (InternalTransaction transaction = (InternalTransaction) database.beginTx()) {
            var cursorContext = transaction.kernelTransaction().cursorContext();
            assertZeroCursor(cursorContext);

            transaction.kernelTransaction().dataWrite().nodeDetachDelete(sourceId);

            assertTraces(cursorContext, isRecordFormat() ? traces(5, 1, 5) : traces(1, 0, 1));
        }
    }

    private void assertZeroCursor(CursorContext cursorContext) {
        assertTraces(cursorContext, traces(0, 0, 0, 0));
    }

    void assertTraces(CursorContext cursorContext, int[] traces) {
        // [pins, unpins, hits, optional faults]
        softly.assertThat(cursorContext.getCursorTracer().pins())
                .as("Number of cursor pins")
                .isEqualTo(traces[0]);
        softly.assertThat(cursorContext.getCursorTracer().unpins())
                .as("Number of cursor unpins")
                .isEqualTo(traces[1]);
        softly.assertThat(cursorContext.getCursorTracer().hits())
                .as("Number of cursor hits")
                .isEqualTo(traces[2]);
        if (traces.length == 4) {
            softly.assertThat(cursorContext.getCursorTracer().faults())
                    .as("Number of cursor faults")
                    .isEqualTo(traces[3]);
        }
    }

    int[] traces(int... traces) {
        return traces;
    }

    boolean isRecordFormat() {
        return database.getDependencyResolver().resolveDependency(StorageEngine.class) instanceof RecordStorageEngine;
    }

    private class CommitCursorChecker extends TransactionEventListenerAdapter<Object> {

        private final CursorContext cursorContext;
        private final int[] traces;
        private volatile boolean invoked;

        CommitCursorChecker(CursorContext cursorContext, int[] traces) {
            this.cursorContext = cursorContext;
            this.traces = traces;
        }

        public boolean isInvoked() {
            return invoked;
        }

        @Override
        public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
            assertTraces(cursorContext, traces);
            invoked = true;
        }
    }
}
