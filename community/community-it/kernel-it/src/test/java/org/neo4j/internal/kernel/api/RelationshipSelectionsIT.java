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
package org.neo4j.internal.kernel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.allIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.incomingIterator;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingCursor;
import static org.neo4j.internal.kernel.api.helpers.RelationshipSelections.outgoingIterator;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class RelationshipSelectionsIT {
    private static final RelationshipType relationshipType = withName("relType");

    @Inject
    private GraphDatabaseAPI database;

    @Test
    void tracePageCacheAccessOnOutgoingCursor() {
        long nodeId = getSparseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = outgoingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingCursor() {
        long nodeId = getSparseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = incomingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllCursor() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = allCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingIterator() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = outgoingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingIterator() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = incomingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllIterator() {
        var nodeId = getSparseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = allIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(4, count(iterator));
                }

                assertCursorHits(cursorContext, 2);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseCursor() {
        long nodeId = getDenseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = outgoingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseCursor() {
        long nodeId = getDenseNodeId();
        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = incomingCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseCursor() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var cursor = allCursor(cursors, nodeCursor, new int[] {typeId}, cursorContext)) {
                    consumeCursor(cursor);
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnOutgoingDenseIterator() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = outgoingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnIncomingDenseIterator() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = incomingIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(2, count(iterator));
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    @Test
    void tracePageCacheAccessOnAllDenseIterator() {
        var nodeId = getDenseNodeId();

        try (var transaction = database.beginTx()) {
            var kernelTransaction = ((InternalTransaction) transaction).kernelTransaction();
            var cursors = kernelTransaction.cursors();
            var typeId = kernelTransaction.tokenRead().relationshipType(relationshipType.name());
            var cursorContext = kernelTransaction.cursorContext();
            try (var nodeCursor = cursors.allocateNodeCursor(cursorContext)) {
                setNodeCursor(nodeId, kernelTransaction, nodeCursor);
                assertCursorHits(cursorContext, 1);

                try (var iterator = allIterator(
                        cursors,
                        nodeCursor,
                        new int[] {typeId},
                        RelationshipDataAccessor::relationshipReference,
                        cursorContext)) {
                    assertEquals(4, count(iterator));
                }

                assertCursorHits(cursorContext, 3);
            }
        }
    }

    private long getSparseNodeId() {
        try (Transaction tx = database.beginTx()) {
            var source = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            source.createRelationshipTo(endNode1, relationshipType);
            source.createRelationshipTo(endNode2, relationshipType);
            endNode1.createRelationshipTo(source, relationshipType);
            endNode2.createRelationshipTo(source, relationshipType);
            long nodeId = source.getId();
            tx.commit();
            return nodeId;
        }
    }

    private long getDenseNodeId() {
        try (Transaction tx = database.beginTx()) {
            var source = tx.createNode();
            var endNode1 = tx.createNode();
            var endNode2 = tx.createNode();
            source.createRelationshipTo(endNode1, relationshipType);
            source.createRelationshipTo(endNode2, relationshipType);
            endNode1.createRelationshipTo(source, relationshipType);
            endNode2.createRelationshipTo(source, relationshipType);

            var other = withName("other");
            for (int i = 0; i < 100; i++) {
                var node = tx.createNode();
                source.createRelationshipTo(node, other);
            }
            long nodeId = source.getId();
            tx.commit();
            return nodeId;
        }
    }

    private static void setNodeCursor(long nodeId, KernelTransaction kernelTransaction, NodeCursor nodeCursor) {
        kernelTransaction.dataRead().singleNode(nodeId, nodeCursor);
        assertTrue(nodeCursor.next());
    }

    private static void consumeCursor(RelationshipTraversalCursor cursor) {
        while (cursor.next()) {
            // consume cursor
        }
    }

    private static void assertCursorHits(CursorContext cursorContext, int atMostHits) {
        PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
        assertThat(cursorTracer.hits()).isLessThanOrEqualTo(atMostHits).isLessThanOrEqualTo(cursorTracer.pins());
        // Since the storage cursor is merely reset(), not closed the state of things is that not all unpins gets
        // registered due to cursor context being closed before the storage cursor on KTI#commit()
    }
}
