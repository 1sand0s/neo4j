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
package org.neo4j.index.internal.gbptree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.contains;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.resultIsFromSlotA;
import static org.neo4j.index.internal.gbptree.TreeNode.DATA_LAYER_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;
import static org.neo4j.index.internal.gbptree.TreeNode.Overflow.NO_NEED_DEFRAG;
import static org.neo4j.index.internal.gbptree.TreeNode.Overflow.YES;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.LEAF;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.index.internal.gbptree.TreeNode.Overflow;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
public abstract class TreeNodeTestBase<KEY, VALUE> {
    static final int STABLE_GENERATION = 1;
    static final int UNSTABLE_GENERATION = 3;
    private static final int HIGH_GENERATION = 4;

    static final int PAGE_SIZE = 512;
    PageAwareByteArrayCursor cursor;

    private TestLayout<KEY, VALUE> layout;
    TreeNode<KEY, VALUE> node;
    private final GenerationKeeper generationTarget = new GenerationKeeper();

    @Inject
    private RandomSupport random;

    @BeforeEach
    void prepareCursor() {
        cursor = new PageAwareByteArrayCursor(PAGE_SIZE);
        cursor.next();
        layout = getLayout();
        OffloadStoreImpl<KEY, VALUE> offloadStore = createOffloadStore();
        node = getNode(PAGE_SIZE, layout, offloadStore);
    }

    OffloadStoreImpl<KEY, VALUE> createOffloadStore() {
        SimpleIdProvider idProvider = new SimpleIdProvider(cursor::duplicate);
        OffloadPageCursorFactory pcFactory = (id, flags, cursorContext) -> cursor.duplicate(id);
        OffloadIdValidator idValidator = OffloadIdValidator.ALWAYS_TRUE;
        return new OffloadStoreImpl<>(layout, idProvider, pcFactory, idValidator, PAGE_SIZE);
    }

    protected abstract TestLayout<KEY, VALUE> getLayout();

    protected abstract TreeNode<KEY, VALUE> getNode(
            int pageSize, Layout<KEY, VALUE> layout, OffloadStore<KEY, VALUE> offloadStore);

    abstract void assertAdditionalHeader(PageCursor cursor, TreeNode<KEY, VALUE> node, int pageSize);

    private KEY key(long seed) {
        return layout.key(seed);
    }

    private VALUE value(long seed) {
        return layout.value(seed);
    }

    @Test
    void shouldInitializeLeaf() {
        // WHEN
        initializeLeaf();

        // THEN
        assertEquals(TreeNode.NODE_TYPE_TREE_NODE, TreeNode.nodeType(cursor));
        assertTrue(TreeNode.isLeaf(cursor));
        assertFalse(TreeNode.isInternal(cursor));
        assertEquals(UNSTABLE_GENERATION, TreeNode.generation(cursor));
        assertEquals(0, TreeNode.keyCount(cursor));
        assertEquals(NO_NODE_FLAG, leftSibling(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertEquals(NO_NODE_FLAG, rightSibling(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertEquals(NO_NODE_FLAG, successor(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertAdditionalHeader(cursor, node, PAGE_SIZE);
    }

    @Test
    void shouldInitializeInternal() {
        // WHEN
        initializeInternal();

        // THEN
        assertEquals(TreeNode.NODE_TYPE_TREE_NODE, TreeNode.nodeType(cursor));
        assertFalse(TreeNode.isLeaf(cursor));
        assertTrue(TreeNode.isInternal(cursor));
        assertEquals(UNSTABLE_GENERATION, TreeNode.generation(cursor));
        assertEquals(0, TreeNode.keyCount(cursor));
        assertEquals(NO_NODE_FLAG, leftSibling(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertEquals(NO_NODE_FLAG, rightSibling(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertEquals(NO_NODE_FLAG, successor(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertAdditionalHeader(cursor, node, PAGE_SIZE);
    }

    @Test
    void shouldWriteAndReadMaxGeneration() {
        // GIVEN
        initializeLeaf();

        // WHEN
        TreeNode.setGeneration(cursor, GenerationSafePointer.MAX_GENERATION);

        // THEN
        long generation = TreeNode.generation(cursor);
        assertEquals(GenerationSafePointer.MAX_GENERATION, generation);
    }

    @Test
    void shouldThrowIfWriteTooLargeGeneration() {
        initializeLeaf();

        assertThrows(
                IllegalArgumentException.class,
                () -> TreeNode.setGeneration(cursor, GenerationSafePointer.MAX_GENERATION + 1));
    }

    @Test
    void shouldThrowIfWriteTooSmallGeneration() {
        initializeLeaf();

        assertThrows(
                IllegalArgumentException.class,
                () -> TreeNode.setGeneration(cursor, GenerationSafePointer.MIN_GENERATION - 1));
    }

    @Test
    void keyValueOperationsInLeaf() throws IOException {
        // GIVEN
        initializeLeaf();
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();

        // WHEN
        KEY firstKey = key(1);
        VALUE firstValue = value(10);
        node.insertKeyValueAt(cursor, firstKey, firstValue, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 1);

        // THEN
        KEY actualKey = node.keyAt(cursor, readKey, 0, LEAF, NULL_CONTEXT);
        assertKeyEquals(firstKey, actualKey);
        assertValueEquals(
                firstValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 0, NULL_CONTEXT).value);

        // WHEN
        KEY secondKey = key(3);
        VALUE secondValue = value(30);
        node.insertKeyValueAt(
                cursor, secondKey, secondValue, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 2);

        // THEN
        assertKeyEquals(firstKey, node.keyAt(cursor, readKey, 0, LEAF, NULL_CONTEXT));
        assertValueEquals(
                firstValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 0, NULL_CONTEXT).value);
        assertKeyEquals(secondKey, node.keyAt(cursor, readKey, 1, LEAF, NULL_CONTEXT));
        assertValueEquals(
                secondValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 1, NULL_CONTEXT).value);

        // WHEN
        KEY removedKey = key(2);
        VALUE removedValue = value(20);
        node.insertKeyValueAt(
                cursor, removedKey, removedValue, 1, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 3);

        // THEN
        assertKeyEquals(firstKey, node.keyAt(cursor, readKey, 0, LEAF, NULL_CONTEXT));
        assertValueEquals(
                firstValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 0, NULL_CONTEXT).value);
        assertKeyEquals(removedKey, node.keyAt(cursor, readKey, 1, LEAF, NULL_CONTEXT));
        assertValueEquals(
                removedValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 1, NULL_CONTEXT).value);
        assertKeyEquals(secondKey, node.keyAt(cursor, readKey, 2, LEAF, NULL_CONTEXT));
        assertValueEquals(
                secondValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 2, NULL_CONTEXT).value);

        // WHEN
        node.removeKeyValueAt(cursor, 1, 3, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 2);

        // THEN
        assertKeyEquals(firstKey, node.keyAt(cursor, readKey, 0, LEAF, NULL_CONTEXT));
        assertValueEquals(
                firstValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 0, NULL_CONTEXT).value);
        assertKeyEquals(secondKey, node.keyAt(cursor, readKey, 1, LEAF, NULL_CONTEXT));
        assertValueEquals(
                secondValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 1, NULL_CONTEXT).value);

        // WHEN
        VALUE overwriteValue = value(666);
        assertTrue(
                node.setValueAt(cursor, overwriteValue, 0, NULL_CONTEXT, STABLE_GENERATION, UNSTABLE_GENERATION),
                String.format("Could not overwrite value, oldValue=%s, newValue=%s", firstValue, overwriteValue));

        // THEN
        assertKeyEquals(firstKey, node.keyAt(cursor, readKey, 0, LEAF, NULL_CONTEXT));
        assertValueEquals(
                overwriteValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 0, NULL_CONTEXT).value);
        assertKeyEquals(secondKey, node.keyAt(cursor, readKey, 1, LEAF, NULL_CONTEXT));
        assertValueEquals(
                secondValue, node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), 1, NULL_CONTEXT).value);
    }

    @Test
    void keyChildOperationsInInternal() throws IOException {
        // GIVEN
        initializeInternal();
        long stable = 3;
        long unstable = 4;
        long zeroChild = 5;

        // WHEN
        node.setChildAt(cursor, zeroChild, 0, stable, unstable);

        // THEN
        assertKeysAndChildren(stable, unstable, zeroChild);

        // WHEN
        long firstKey = 1;
        long firstChild = 10;
        node.insertKeyAndRightChildAt(cursor, key(firstKey), firstChild, 0, 0, stable, unstable, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 1);

        // THEN
        assertKeysAndChildren(stable, unstable, zeroChild, firstKey, firstChild);

        // WHEN
        long secondKey = 3;
        long secondChild = 30;
        node.insertKeyAndRightChildAt(cursor, key(secondKey), secondChild, 1, 1, stable, unstable, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 2);

        // THEN
        assertKeysAndChildren(stable, unstable, zeroChild, firstKey, firstChild, secondKey, secondChild);

        // WHEN
        long removedKey = 2;
        long removedChild = 20;
        node.insertKeyAndRightChildAt(cursor, key(removedKey), removedChild, 1, 2, stable, unstable, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 3);

        // THEN
        assertKeysAndChildren(
                stable, unstable, zeroChild, firstKey, firstChild, removedKey, removedChild, secondKey, secondChild);

        // WHEN
        node.removeKeyAndRightChildAt(cursor, 1, 3, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 2);

        // THEN
        assertKeysAndChildren(stable, unstable, zeroChild, firstKey, firstChild, secondKey, secondChild);

        // WHEN
        node.removeKeyAndLeftChildAt(cursor, 0, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 1);

        // THEN
        assertKeysAndChildren(stable, unstable, firstChild, secondKey, secondChild);

        // WHEN
        long overwriteChild = 666;
        node.setChildAt(cursor, overwriteChild, 0, stable, unstable);

        // THEN
        assertKeysAndChildren(stable, unstable, overwriteChild, secondKey, secondChild);
    }

    @Test
    void shouldFillInternal() throws IOException {
        initializeInternal();
        long stable = 3;
        long unstable = 4;
        int keyCount = 0;
        long childId = 10;
        node.setChildAt(cursor, childId, 0, stable, unstable);
        childId++;
        KEY key = key(childId);
        for (; node.internalOverflow(cursor, keyCount, key) == Overflow.NO; childId++, keyCount++, key = key(childId)) {
            node.insertKeyAndRightChildAt(cursor, key, childId, keyCount, keyCount, stable, unstable, NULL_CONTEXT);
        }

        // Assert children
        long firstChild = 10;
        for (int i = 0; i <= keyCount; i++) {
            assertEquals(firstChild + i, pointer(node.childAt(cursor, i, stable, unstable)));
        }

        // Assert keys
        int firstKey = 11;
        KEY readKey = layout.newKey();
        for (int i = 0; i < keyCount; i++) {
            assertKeyEquals(key(firstKey + i), node.keyAt(cursor, readKey, i, INTERNAL, NULL_CONTEXT));
        }
    }

    @Test
    void shouldSetAndGetKeyCount() {
        // GIVEN
        initializeLeaf();
        assertEquals(0, TreeNode.keyCount(cursor));

        // WHEN
        int keyCount = 5;
        TreeNode.setKeyCount(cursor, keyCount);

        // THEN
        assertEquals(keyCount, TreeNode.keyCount(cursor));
    }

    @Test
    void shouldSetAndGetSiblings() {
        // GIVEN
        initializeLeaf();

        // WHEN
        TreeNode.setLeftSibling(cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION);
        TreeNode.setRightSibling(cursor, 456, STABLE_GENERATION, UNSTABLE_GENERATION);

        // THEN
        assertEquals(123, leftSibling(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
        assertEquals(456, rightSibling(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
    }

    @Test
    void shouldSetAndGetSuccessor() {
        // GIVEN
        initializeLeaf();

        // WHEN
        TreeNode.setSuccessor(cursor, 123, STABLE_GENERATION, UNSTABLE_GENERATION);

        // THEN
        assertEquals(123, successor(cursor, STABLE_GENERATION, UNSTABLE_GENERATION));
    }

    @Test
    void shouldDefragLeafWithTombstoneOnLast() throws IOException {
        // GIVEN
        initializeLeaf();
        KEY key = key(1);
        VALUE value = value(1);
        node.insertKeyValueAt(cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(2);
        value = value(2);
        node.insertKeyValueAt(cursor, key, value, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // AND
        node.removeKeyValueAt(cursor, 1, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 1);

        // WHEN
        node.defragmentLeaf(cursor);

        // THEN
        assertKeyEquals(key(1), node.keyAt(cursor, layout.newKey(), 0, LEAF, NULL_CONTEXT));
    }

    @Test
    void shouldDefragLeafWithTombstoneOnFirst() throws IOException {
        // GIVEN
        initializeLeaf();
        KEY key = key(1);
        VALUE value = value(1);
        node.insertKeyValueAt(cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(2);
        value = value(2);
        node.insertKeyValueAt(cursor, key, value, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // AND
        node.removeKeyValueAt(cursor, 0, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 1);

        // WHEN
        node.defragmentLeaf(cursor);

        // THEN
        assertKeyEquals(key(2), node.keyAt(cursor, layout.newKey(), 0, LEAF, NULL_CONTEXT));
    }

    @Test
    void shouldDefragLeafWithTombstoneInterleaved() throws IOException {
        // GIVEN
        initializeLeaf();
        KEY key = key(1);
        VALUE value = value(1);
        node.insertKeyValueAt(cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(2);
        value = value(2);
        node.insertKeyValueAt(cursor, key, value, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(3);
        value = value(3);
        node.insertKeyValueAt(cursor, key, value, 2, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // AND
        node.removeKeyValueAt(cursor, 1, 3, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 2);

        // WHEN
        node.defragmentLeaf(cursor);

        // THEN
        assertKeyEquals(key(1), node.keyAt(cursor, layout.newKey(), 0, LEAF, NULL_CONTEXT));
        assertKeyEquals(key(3), node.keyAt(cursor, layout.newKey(), 1, LEAF, NULL_CONTEXT));
    }

    @Test
    void shouldDefragLeafWithMultipleTombstonesInterleavedOdd() throws IOException {
        // GIVEN
        initializeLeaf();
        KEY key = key(1);
        VALUE value = value(1);
        node.insertKeyValueAt(cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(2);
        value = value(2);
        node.insertKeyValueAt(cursor, key, value, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(3);
        value = value(3);
        node.insertKeyValueAt(cursor, key, value, 2, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(4);
        value = value(4);
        node.insertKeyValueAt(cursor, key, value, 3, 3, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(5);
        value = value(5);
        node.insertKeyValueAt(cursor, key, value, 4, 4, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // AND
        node.removeKeyValueAt(cursor, 1, 5, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        node.removeKeyValueAt(cursor, 2, 4, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 3);

        // WHEN
        node.defragmentLeaf(cursor);

        // THEN
        assertKeyEquals(key(1), node.keyAt(cursor, layout.newKey(), 0, LEAF, NULL_CONTEXT));
        assertKeyEquals(key(3), node.keyAt(cursor, layout.newKey(), 1, LEAF, NULL_CONTEXT));
        assertKeyEquals(key(5), node.keyAt(cursor, layout.newKey(), 2, LEAF, NULL_CONTEXT));
    }

    @Test
    void shouldDefragLeafWithMultipleTombstonesInterleavedEven() throws IOException {
        // GIVEN
        initializeLeaf();
        KEY key = key(1);
        VALUE value = value(1);
        node.insertKeyValueAt(cursor, key, value, 0, 0, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(2);
        value = value(2);
        node.insertKeyValueAt(cursor, key, value, 1, 1, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(3);
        value = value(3);
        node.insertKeyValueAt(cursor, key, value, 2, 2, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(4);
        value = value(4);
        node.insertKeyValueAt(cursor, key, value, 3, 3, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        key = key(5);
        value = value(5);
        node.insertKeyValueAt(cursor, key, value, 4, 4, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);

        // AND
        node.removeKeyValueAt(cursor, 0, 5, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        node.removeKeyValueAt(cursor, 1, 4, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        node.removeKeyValueAt(cursor, 2, 3, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
        TreeNode.setKeyCount(cursor, 2);

        // WHEN
        node.defragmentLeaf(cursor);

        // THEN
        assertKeyEquals(key(2), node.keyAt(cursor, layout.newKey(), 0, LEAF, NULL_CONTEXT));
        assertKeyEquals(key(4), node.keyAt(cursor, layout.newKey(), 1, LEAF, NULL_CONTEXT));
    }

    @Test
    void shouldInsertAndRemoveRandomKeysAndValues() throws IOException {
        // This test doesn't care about sorting, that's an aspect that lies outside of TreeNode, really

        // GIVEN
        initializeLeaf();
        // add +1 to these to simplify some array logic in the test itself
        List<KEY> expectedKeys = new ArrayList<>();
        List<VALUE> expectedValues = new ArrayList<>();
        int expectedKeyCount = 0;
        KEY readKey = layout.newKey();
        VALUE readValue = layout.newValue();

        // WHEN/THEN
        for (int i = 0; i < 1000; i++) {
            if (random.nextFloat() < 0.7) { // 70% insert
                KEY newKey;
                do {
                    newKey = key(random.nextLong());
                } while (contains(expectedKeys, newKey, layout));
                VALUE newValue = value(random.nextLong());

                Overflow overflow = node.leafOverflow(cursor, expectedKeyCount, newKey, newValue);
                if (overflow == NO_NEED_DEFRAG) {
                    node.defragmentLeaf(cursor);
                }
                if (overflow != YES) { // there's room
                    int position = expectedKeyCount == 0 ? 0 : random.nextInt(expectedKeyCount);
                    // ensure unique
                    node.insertKeyValueAt(
                            cursor,
                            newKey,
                            newValue,
                            position,
                            expectedKeyCount,
                            STABLE_GENERATION,
                            UNSTABLE_GENERATION,
                            NULL_CONTEXT);
                    expectedKeys.add(position, newKey);
                    expectedValues.add(position, newValue);

                    TreeNode.setKeyCount(cursor, ++expectedKeyCount);
                }
            } else { // 30% remove
                if (expectedKeyCount > 0) { // there are things to remove
                    int position = random.nextInt(expectedKeyCount);
                    node.keyAt(cursor, readKey, position, LEAF, NULL_CONTEXT);
                    node.valueAt(cursor, new TreeNode.ValueHolder<>(readValue), position, NULL_CONTEXT);
                    node.removeKeyValueAt(
                            cursor, position, expectedKeyCount, STABLE_GENERATION, UNSTABLE_GENERATION, NULL_CONTEXT);
                    KEY expectedKey = expectedKeys.remove(position);
                    VALUE expectedValue = expectedValues.remove(position);
                    assertEquals(
                            0,
                            layout.compare(expectedKey, readKey),
                            String.format(
                                    "Key differ with expected%n    readKey=%s %nexpectedKey=%s%n",
                                    readKey, expectedKey));
                    assertEquals(
                            0,
                            layout.compareValue(expectedValue, readValue),
                            "Value differ with expected, value=" + readValue + ", expectedValue=" + expectedValue);

                    TreeNode.setKeyCount(cursor, --expectedKeyCount);
                }
            }
        }

        // THEN
        assertContent(expectedKeys, expectedValues, expectedKeyCount);
    }

    private void assertContent(List<KEY> expectedKeys, List<VALUE> expectedValues, int expectedKeyCount)
            throws IOException {
        KEY actualKey = layout.newKey();
        VALUE actualValue = layout.newValue();
        assertEquals(expectedKeyCount, TreeNode.keyCount(cursor));
        for (int i = 0; i < expectedKeyCount; i++) {
            KEY expectedKey = expectedKeys.get(i);
            node.keyAt(cursor, actualKey, i, LEAF, NULL_CONTEXT);
            assertEquals(
                    0,
                    layout.compare(expectedKey, actualKey),
                    "Key differ with expected, actualKey=" + actualKey + ", expectedKey=" + expectedKey);

            VALUE expectedValue = expectedValues.get(i);
            node.valueAt(cursor, new TreeNode.ValueHolder<>(actualValue), i, NULL_CONTEXT);
            assertEquals(
                    0,
                    layout.compareValue(expectedValue, actualValue),
                    "Value differ with expected, actualValue=" + actualValue + ", expectedValue=" + expectedValue);
        }
    }

    @Test
    void shouldAssertPageSizeBigEnoughForAtLeastTwoKeys() {
        assertThrows(
                MetadataMismatchException.class,
                () -> new TreeNodeFixedSize<>(
                        TreeNode.BASE_HEADER_LENGTH + layout.keySize(null) + layout.valueSize(null), layout));
    }

    @Test
    void shouldReadPointerGenerationFromAbsoluteOffsetSlotA() {
        // GIVEN
        long generation = UNSTABLE_GENERATION;
        long pointer = 12;
        TreeNode.setRightSibling(cursor, pointer, STABLE_GENERATION, generation);

        // WHEN
        long readResult = TreeNode.rightSibling(cursor, STABLE_GENERATION, generation, generationTarget);
        long readGeneration = generationTarget.generation;

        // THEN
        assertEquals(pointer, pointer(readResult));
        assertEquals(generation, readGeneration);
        assertTrue(resultIsFromSlotA(readResult));
    }

    @Test
    void shouldReadPointerGenerationFromAbsoluteOffsetSlotB() {
        // GIVEN
        long generation = HIGH_GENERATION;
        long oldPointer = 12;
        long pointer = 123;
        TreeNode.setRightSibling(cursor, oldPointer, STABLE_GENERATION, UNSTABLE_GENERATION);
        TreeNode.setRightSibling(cursor, pointer, UNSTABLE_GENERATION, generation);

        // WHEN
        long readResult = TreeNode.rightSibling(cursor, UNSTABLE_GENERATION, generation, generationTarget);
        long readGeneration = generationTarget.generation;

        // THEN
        assertEquals(pointer, pointer(readResult));
        assertEquals(generation, readGeneration);
        assertFalse(resultIsFromSlotA(readResult));
    }

    @Test
    void shouldReadPointerGenerationFromLogicalPosSlotA() {
        // GIVEN
        long generation = UNSTABLE_GENERATION;
        long pointer = 12;
        int childPos = 2;
        node.setChildAt(cursor, pointer, childPos, STABLE_GENERATION, generation);

        // WHEN
        long readResult = node.childAt(cursor, childPos, STABLE_GENERATION, generation, generationTarget);
        long readGeneration = generationTarget.generation;

        // THEN
        assertEquals(pointer, pointer(readResult));
        assertEquals(generation, readGeneration);
        assertTrue(resultIsFromSlotA(readResult));
    }

    @Test
    void shouldReadPointerGenerationFromLogicalPosZeroSlotA() {
        // GIVEN
        long generation = UNSTABLE_GENERATION;
        long pointer = 12;
        int childPos = 0;
        node.setChildAt(cursor, pointer, childPos, STABLE_GENERATION, generation);

        // WHEN
        long readResult = node.childAt(cursor, childPos, STABLE_GENERATION, generation, generationTarget);
        long readGeneration = generationTarget.generation;

        // THEN
        assertEquals(pointer, pointer(readResult));
        assertEquals(generation, readGeneration);
        assertTrue(resultIsFromSlotA(readResult));
    }

    @Test
    void shouldReadPointerGenerationFromLogicalPosZeroSlotB() {
        // GIVEN
        long generation = HIGH_GENERATION;
        long oldPointer = 13;
        long pointer = 12;
        int childPos = 0;
        node.setChildAt(cursor, oldPointer, childPos, STABLE_GENERATION, UNSTABLE_GENERATION);
        node.setChildAt(cursor, pointer, childPos, UNSTABLE_GENERATION, generation);

        // WHEN
        long readResult = node.childAt(cursor, childPos, UNSTABLE_GENERATION, generation, generationTarget);
        long readGeneration = generationTarget.generation;

        // THEN
        assertEquals(pointer, pointer(readResult));
        assertEquals(generation, readGeneration);
        assertFalse(resultIsFromSlotA(readResult));
    }

    @Test
    void shouldReadPointerGenerationFromLogicalPosSlotB() {
        // GIVEN
        long generation = HIGH_GENERATION;
        long oldPointer = 12;
        long pointer = 123;
        int childPos = 2;
        node.setChildAt(cursor, oldPointer, childPos, STABLE_GENERATION, UNSTABLE_GENERATION);
        node.setChildAt(cursor, pointer, childPos, UNSTABLE_GENERATION, generation);

        // WHEN
        long readResult = node.childAt(cursor, childPos, UNSTABLE_GENERATION, generation, generationTarget);
        long readGeneration = generationTarget.generation;

        // THEN
        assertEquals(pointer, pointer(readResult));
        assertEquals(generation, readGeneration);
        assertFalse(resultIsFromSlotA(readResult));
    }

    private void assertKeyEquals(KEY expectedKey, KEY actualKey) {
        assertEquals(
                0,
                layout.compare(expectedKey, actualKey),
                String.format("expectedKey=%s, actualKey=%s", expectedKey, actualKey));
    }

    private void assertValueEquals(VALUE expectedValue, VALUE actualValue) {
        assertEquals(
                0,
                layout.compareValue(expectedValue, actualValue),
                String.format("expectedValue=%s, actualKey=%s", expectedValue, actualValue));
    }

    private void assertKeysAndChildren(long stable, long unstable, long... keysAndChildren) {
        KEY actualKey = layout.newKey();
        int pos;
        for (int i = 0; i < keysAndChildren.length; i++) {
            pos = i / 2;
            if (i % 2 == 0) {
                assertEquals(
                        keysAndChildren[i],
                        GenerationSafePointerPair.pointer(node.childAt(cursor, pos, stable, unstable)));
            } else {
                KEY expectedKey = key(keysAndChildren[i]);
                node.keyAt(cursor, actualKey, pos, INTERNAL, NULL_CONTEXT);
                assertEquals(0, layout.compare(expectedKey, actualKey));
            }
        }
    }

    private void initializeLeaf() {
        node.initializeLeaf(cursor, DATA_LAYER_FLAG, STABLE_GENERATION, UNSTABLE_GENERATION);
    }

    private void initializeInternal() {
        node.initializeInternal(cursor, DATA_LAYER_FLAG, STABLE_GENERATION, UNSTABLE_GENERATION);
    }

    private static long rightSibling(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return pointer(TreeNode.rightSibling(cursor, stableGeneration, unstableGeneration));
    }

    private static long leftSibling(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return pointer(TreeNode.leftSibling(cursor, stableGeneration, unstableGeneration));
    }

    private static long successor(PageCursor cursor, long stableGeneration, long unstableGeneration) {
        return pointer(TreeNode.successor(cursor, stableGeneration, unstableGeneration));
    }
}
