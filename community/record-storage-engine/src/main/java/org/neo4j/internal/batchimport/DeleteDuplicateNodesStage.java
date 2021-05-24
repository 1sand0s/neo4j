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
package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.internal.batchimport.cache.idmapping.IdMapper;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.NeoStores;

/**
 * After {@link IdMapper#prepare(PropertyValueLookup, Collector, ProgressListener)} any duplicate input ids have been
 * detected, i.e. also duplicate imported nodes. This stage makes one pass over those duplicate node ids
 * and deletes from from the store(s).
 */
public class DeleteDuplicateNodesStage extends Stage
{
    public DeleteDuplicateNodesStage( Configuration config, LongIterator duplicateNodeIds,
            NeoStores neoStore, DataImporter.Monitor storeMonitor, PageCacheTracer pageCacheTracer )
    {
        super( "DEDUP", null, config, 0 );
        add( new DeleteDuplicateNodesStep( control(), config, duplicateNodeIds, neoStore,
                storeMonitor, pageCacheTracer ) );
    }
}
