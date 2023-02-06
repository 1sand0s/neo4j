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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import org.neo4j.io.fs.PositionableChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChecksumChannel;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.util.FeatureToggles;

/**
 * Reads {@link LogEntry log entries} off of a channel. Supported versions can be read intermixed.
 */
public class VersionAwareLogEntryReader implements LogEntryReader {
    private static final boolean VERIFY_CHECKSUM_CHAIN =
            FeatureToggles.flag(LogEntryReader.class, "verifyChecksumChain", false);
    private final CommandReaderFactory commandReaderFactory;
    private final LogPositionMarker positionMarker;
    private final boolean verifyChecksumChain;
    private LogEntryParserSet parserSet;
    private int lastTxChecksum = BASE_TX_CHECKSUM;

    public VersionAwareLogEntryReader(CommandReaderFactory commandReaderFactory) {
        this(commandReaderFactory, true);
    }

    public VersionAwareLogEntryReader(CommandReaderFactory commandReaderFactory, boolean verifyChecksumChain) {
        this.commandReaderFactory = commandReaderFactory;
        this.positionMarker = new LogPositionMarker();
        this.verifyChecksumChain = verifyChecksumChain;
    }

    @Override
    public LogEntry readLogEntry(ReadableClosablePositionAwareChecksumChannel channel) throws IOException {
        try {
            byte versionCode = channel.markAndGet(positionMarker);
            if (versionCode == 0) {
                // we reached the end of available records but still have space available in pre-allocated file
                // we reset channel position to restore last read byte in case someone would like to re-read or check it
                // again if possible
                // and we report that we reach end of record stream from our point of view
                if (channel instanceof PositionableChannel) {
                    rewindOneByte(channel);
                } else {
                    throw new IllegalStateException(
                            "Log reader expects positionable channel to be able to reset offset. Current channel: "
                                    + channel);
                }
                return null;
            }
            if (parserSet == null || parserSet.getIntroductionVersion().version() != versionCode) {
                try {
                    parserSet = LogEntryParserSets.parserSet(KernelVersion.getForVersion(versionCode));
                } catch (IllegalArgumentException e) {
                    String msg;
                    if (versionCode > KernelVersion.LATEST.version()) {
                        msg = String.format(
                                "Log file contains entries with prefix %d, and the highest supported prefix is %s. This "
                                        + "indicates that the log files originates from an newer version of neo4j, which we don't support "
                                        + "downgrading from.",
                                versionCode, KernelVersion.LATEST);
                    } else {
                        msg = String.format(
                                "Log file contains entries with prefix %d, and the lowest supported prefix is %s. This "
                                        + "indicates that the log files originates from an older version of neo4j, which we don't support "
                                        + "migrations from.",
                                versionCode, KernelVersion.EARLIEST);
                    }
                    throw new UnsupportedLogVersionException(msg);
                }
                // Since checksum is calculated over the whole entry we need to rewind and begin
                // a new checksum segment if we change version parser.
                if (channel instanceof PositionableChannel) {
                    rewindOneByte(channel);
                    channel.beginChecksum();
                    channel.get();
                }
            }

            byte typeCode = channel.get();

            LogEntry entry;
            try {
                var entryReader = parserSet.select(typeCode);
                entry = entryReader.parse(
                        parserSet.getIntroductionVersion(),
                        parserSet.wrap(channel),
                        positionMarker,
                        commandReaderFactory);
            } catch (ReadPastEndException e) { // Make these exceptions slip by straight out to the outer handler
                throw e;
            } catch (Exception e) { // Tag all other exceptions with log position and other useful information
                LogPosition position = positionMarker.newPosition();
                var message = e.getMessage() + ". At position " + position + " and entry version " + versionCode;
                if (e instanceof UnsupportedLogVersionException) {
                    throw new UnsupportedLogVersionException(message, e);
                }
                throw new IOException(message, e);
            }

            verifyChecksumChain(entry);
            return entry;
        } catch (ReadPastEndException e) {
            return null;
        }
    }

    private void verifyChecksumChain(LogEntry e) {
        if (VERIFY_CHECKSUM_CHAIN && verifyChecksumChain) {
            if (e instanceof LogEntryStart logEntryStart) {
                int previousChecksum = logEntryStart.getPreviousChecksum();
                if (lastTxChecksum != BASE_TX_CHECKSUM) {
                    if (previousChecksum != lastTxChecksum) {
                        throw new IllegalStateException("The checksum chain is broken. " + positionMarker);
                    }
                }
            } else if (e instanceof LogEntryCommit logEntryCommit) {
                lastTxChecksum = logEntryCommit.getChecksum();
            }
        }
    }

    private void rewindOneByte(ReadableClosablePositionAwareChecksumChannel channel) throws IOException {
        // take current position
        channel.getCurrentPosition(positionMarker);
        ((PositionableChannel) channel).setCurrentPosition(positionMarker.getByteOffset() - 1);
        // refresh with reset position
        channel.getCurrentPosition(positionMarker);
    }

    @Override
    public LogPosition lastPosition() {
        return positionMarker.newPosition();
    }
}
