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
package org.neo4j.kernel.impl.transaction.log.entry.v50;

import static java.lang.Math.min;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0;
import static org.neo4j.kernel.impl.transaction.log.entry.v50.DetachedCheckpointLogEntryParserV5_0.MAX_DESCRIPTION_LENGTH;
import static org.neo4j.storageengine.api.StoreIdSerialization.MAX_STORE_ID_LENGTH;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.CheckpointLogEntryWriter;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdSerialization;
import org.neo4j.storageengine.api.TransactionId;

public class DetachedCheckpointLogEntryWriterV5_0 implements CheckpointLogEntryWriter {
    public static final int RECORD_LENGTH_BYTES = 232;
    protected final WritableChecksumChannel channel;

    public DetachedCheckpointLogEntryWriterV5_0(WritableChecksumChannel channel) {
        this.channel = channel;
    }

    @Override
    public void writeCheckPointEntry(
            TransactionId transactionId,
            KernelVersion kernelVersion,
            LogPosition logPosition,
            Instant checkpointTime,
            StoreId storeId,
            String reason)
            throws IOException {
        channel.beginChecksum();
        writeLogEntryHeader(kernelVersion, DETACHED_CHECK_POINT_V5_0, channel);
        byte[] storeIdBuffer = new byte[MAX_STORE_ID_LENGTH];
        StoreIdSerialization.serializeWithFixedSize(storeId, ByteBuffer.wrap(storeIdBuffer));
        byte[] reasonBytes = reason.getBytes();
        short length = safeCastIntToShort(min(reasonBytes.length, MAX_DESCRIPTION_LENGTH));
        byte[] descriptionBytes = new byte[MAX_DESCRIPTION_LENGTH];
        System.arraycopy(reasonBytes, 0, descriptionBytes, 0, length);
        channel.putLong(logPosition.getLogVersion())
                .putLong(logPosition.getByteOffset())
                .putLong(checkpointTime.toEpochMilli())
                .put(storeIdBuffer, storeIdBuffer.length)
                .putLong(transactionId.transactionId())
                .putInt(transactionId.checksum())
                .putLong(transactionId.commitTimestamp())
                .putShort(length)
                .put(descriptionBytes, descriptionBytes.length);
        channel.putChecksum();
    }

    protected static void writeLogEntryHeader(KernelVersion kernelVersion, byte type, WritableChannel channel)
            throws IOException {
        channel.put(kernelVersion.version()).put(type);
    }
}
