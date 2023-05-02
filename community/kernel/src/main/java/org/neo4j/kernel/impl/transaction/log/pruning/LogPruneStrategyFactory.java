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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.parse;

import java.time.Clock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.util.VisibleForTesting;

public class LogPruneStrategyFactory {
    static final LogPruneStrategy NO_PRUNING = new LogPruneStrategy() {
        @Override
        public VersionRange findLogVersionsToDelete(long upToLogVersion) {
            // Never delete anything.
            return LogPruneStrategy.EMPTY_RANGE;
        }

        @Override
        public String toString() {
            return "keep_all";
        }
    };

    public LogPruneStrategyFactory() {}

    /**
     * Parses a configuration value for log specifying log pruning. It has one of these forms:
     * <ul>
     *   <li>all</li>
     *   <li>[number][unit] [type]</li>
     * </ul>
     * For example:
     * <ul>
     *   <li>100M size - For keeping last 100 mebibytes of log data</li>
     *   <li>20 pcs - For keeping last 20 non-empty log files</li>
     *   <li>7 days - For keeping last 7 days worth of log data</li>
     *   <li>1k hours - For keeping last 1000 hours worth of log data</li>
     * </ul>
     */
    LogPruneStrategy strategyFromConfigValue(
            FileSystemAbstraction fileSystem,
            LogFiles logFiles,
            InternalLogProvider logProvider,
            Clock clock,
            String configValue) {
        ThresholdConfigValue value = parse(configValue);

        if (value == ThresholdConfigValue.NO_PRUNING) {
            return NO_PRUNING;
        }

        Threshold thresholdToUse = getThresholdByType(fileSystem, logProvider, clock, value, configValue);
        return new ThresholdBasedPruneStrategy(logFiles.getLogFile(), thresholdToUse);
    }

    @VisibleForTesting
    static Threshold getThresholdByType(
            FileSystemAbstraction fileSystem,
            InternalLogProvider logProvider,
            Clock clock,
            ThresholdConfigValue value,
            String originalConfigValue) {
        long thresholdValue = value.value;

        return switch (value.type) {
            case "files" -> new FileCountThreshold(thresholdValue);
            case "size" -> new FileSizeThreshold(fileSystem, thresholdValue);
                // txs and entries are synonyms
            case "txs", "entries" -> new EntryCountThreshold(logProvider, thresholdValue);
            case "hours" -> new EntryTimespanThreshold(logProvider, clock, HOURS, thresholdValue);
            case "days" -> new EntryTimespanThreshold(logProvider, clock, DAYS, thresholdValue);
            default -> throw new IllegalArgumentException(
                    "Invalid log pruning configuration value '" + originalConfigValue + "'. Invalid type '" + value.type
                            + "', valid are files, size, txs, entries, hours, days.");
        };
    }
}
