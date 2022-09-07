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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.io.os.OsBeanUtil.VALUE_UNAVAILABLE;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.os.OsBeanUtil;

class ConfigurationTest {
    @Test
    void shouldOverrideBigPageCacheMemorySettingContainingUnit() {
        // GIVEN
        Config dbConfig = Config.defaults(pagecache_memory, ByteUnit.gibiBytes(2));
        Configuration config = new Configuration.Overridden(dbConfig);

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertEquals(Configuration.MAX_PAGE_CACHE_MEMORY, memory);
    }

    @Test
    void shouldOverrideSmallPageCacheMemorySettingContainingUnit() {
        // GIVEN
        long overridden = ByteUnit.mebiBytes(10);
        Config dbConfig = Config.defaults(pagecache_memory, overridden);
        Configuration config = new Configuration.Overridden(dbConfig);

        // WHEN
        long memory = config.pageCacheMemory();

        // THEN
        assertEquals(overridden, memory);
    }

    @Test
    void shouldCalculateCorrectMaxMemorySetting() {
        long freeMachineMemory = OsBeanUtil.getFreePhysicalMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        assumeTrue(freeMachineMemory != VALUE_UNAVAILABLE);

        // given
        int percent = 70;
        Configuration config = new Configuration() {
            @Override
            public long maxOffHeapMemory() {
                return Configuration.calculateMaxMemoryFromPercent(percent, freeMachineMemory, maxMemory);
            }
        };

        // when
        long memory = config.maxOffHeapMemory();

        // then
        long expected = Math.round((freeMachineMemory - maxMemory) * (percent / 100D));
        assertThat(memory).isEqualTo(expected);
    }
}
