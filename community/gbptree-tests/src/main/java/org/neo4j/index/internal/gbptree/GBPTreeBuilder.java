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

import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_MONITOR;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.Consumer;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree.Monitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

/**
 * Convenient builder for a {@link GBPTree}. Either created using zero-argument constructor for maximum
 * flexibility, or with constructor with arguments considered mandatory to be able to build a proper tree.
 *
 * @param <KEY> type of key in {@link GBPTree}
 * @param <VALUE> type of value in {@link GBPTree}
 */
public class GBPTreeBuilder<ROOT_KEY, KEY, VALUE> {
    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystem;
    private Path path;
    private Monitor monitor = NO_MONITOR;
    private Header.Reader headerReader = NO_HEADER_READER;
    private Layout<KEY, VALUE> dataLayout;
    private KeyLayout<ROOT_KEY> rootLayout;
    private Consumer<PageCursor> headerWriter = NO_HEADER_WRITER;
    private RecoveryCleanupWorkCollector recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.immediate();
    private DatabaseReadOnlyChecker readOnlyChecker = DatabaseReadOnlyChecker.writable();
    private PageCacheTracer pageCacheTracer = NULL;
    private ImmutableSet<OpenOption> openOptions = immutable.empty();
    private TreeNodeLayoutFactory treeNodeLayoutFactory = TreeNodeLayoutFactory.getInstance();

    public GBPTreeBuilder(
            PageCache pageCache, FileSystemAbstraction fileSystem, Path path, Layout<KEY, VALUE> dataLayout) {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        with(path);
        with(dataLayout);
    }

    public GBPTreeBuilder(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Path path,
            Layout<KEY, VALUE> dataLayout,
            KeyLayout<ROOT_KEY> rootLayout) {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        with(path);
        with(dataLayout);
        withRootLayout(rootLayout);
        treeNodeLayoutFactory = TreeNodeLayoutFactory.getInstance();
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(Layout<KEY, VALUE> layout) {
        this.dataLayout = layout;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> withRootLayout(KeyLayout<ROOT_KEY> layout) {
        this.rootLayout = layout;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(Path file) {
        this.path = file;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(Monitor monitor) {
        this.monitor = monitor;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(Header.Reader headerReader) {
        this.headerReader = headerReader;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(Consumer<PageCursor> headerWriter) {
        this.headerWriter = headerWriter;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(RecoveryCleanupWorkCollector recoveryCleanupWorkCollector) {
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(DatabaseReadOnlyChecker readOnlyChecker) {
        this.readOnlyChecker = readOnlyChecker;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(PageCacheTracer pageCacheTracer) {
        this.pageCacheTracer = pageCacheTracer;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(ImmutableSet<OpenOption> openOptions) {
        this.openOptions = openOptions;
        return this;
    }

    public GBPTreeBuilder<ROOT_KEY, KEY, VALUE> with(TreeNodeLayoutFactory treeNodeLayoutFactory) {
        this.treeNodeLayoutFactory = treeNodeLayoutFactory;
        return this;
    }

    public GBPTree<KEY, VALUE> build() {
        CursorContextFactory cursorContextFactory =
                new CursorContextFactory(pageCacheTracer, EmptyVersionContextSupplier.EMPTY);
        return new GBPTree<>(
                pageCache,
                fileSystem,
                path,
                dataLayout,
                monitor,
                headerReader,
                headerWriter,
                recoveryCleanupWorkCollector,
                readOnlyChecker,
                openOptions,
                DEFAULT_DATABASE_NAME,
                "test tree",
                cursorContextFactory,
                pageCacheTracer,
                treeNodeLayoutFactory);
    }

    public MultiRootGBPTree<ROOT_KEY, KEY, VALUE> buildMultiRoot() {
        var cursorContextFactory = new CursorContextFactory(pageCacheTracer, EmptyVersionContextSupplier.EMPTY);
        return new MultiRootGBPTree<>(
                pageCache,
                fileSystem,
                path,
                dataLayout,
                monitor,
                headerReader,
                headerWriter,
                recoveryCleanupWorkCollector,
                readOnlyChecker,
                openOptions,
                DEFAULT_DATABASE_NAME,
                "test tree",
                cursorContextFactory,
                RootLayerConfiguration.multipleRoots(rootLayout, (int) kibiBytes(10)),
                pageCacheTracer,
                treeNodeLayoutFactory);
    }
}
