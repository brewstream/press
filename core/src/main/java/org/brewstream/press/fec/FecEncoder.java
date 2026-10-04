/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brewstream.press.fec;

import io.netty.buffer.ByteBufAllocator;
import org.brewstream.press.packet.FecPacket;
import org.brewstream.press.packet.RtpPacket;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates SMPTE 2022-1 FEC for a media stream: an L-by-D matrix of packets,
 * filled row by row, with one row FEC packet per row and one column FEC packet
 * per column.
 *
 * <p><b>When packets go out.</b> A row FEC packet is emitted as soon as its row
 * is complete. Column FEC packets are held until their matrix is complete, then
 * emitted one every D media packets during the next matrix, as ffmpeg does.
 * Sending them together would put a burst of L FEC packets on the wire. A
 * network that drops bursts would then lose many of them at once, which is the
 * failure column FEC exists to survive.
 *
 * <p>Expects media packets in sending order with consecutive sequence numbers.
 * Not thread-safe.
 */
public final class FecEncoder {

    /** Where FEC packets go. Each packet's payload belongs to the sink. */
    public interface Sink {
        void column(FecPacket packet);

        void row(FecPacket packet);
    }

    /** SMPTE 2022-1's limits: L and D each from 1 to 20 (D at least 4), L times D at most 100. */
    public static void validate(int columns, int rows) {
        if (columns < 1 || columns > 20 || rows < 4 || rows > 20 || columns * rows > 100) {
            throw new IllegalArgumentException("FEC matrix " + columns + "x" + rows
                    + " outside SMPTE 2022-1 limits: L 1-20, D 4-20, L*D at most 100");
        }
    }

    private final int columns;
    private final int rows;
    private final Sink sink;
    private final ByteBufAllocator allocator;

    private final XorAccumulator rowXor;
    private int rowBase;
    private long rowTimestamp;
    private final XorAccumulator[] columnXor;
    private final int[] columnBase;
    private final long[] columnTimestamp;
    private final Queue<FecPacket> heldColumns = new ArrayDeque<>();

    private int index;
    private int rowSequence = ThreadLocalRandom.current().nextInt(0x1000);
    private int columnSequence = ThreadLocalRandom.current().nextInt(0x1000);

    /**
     * @param columns L, the row length and the column FEC packets' offset
     * @param rows    D, the column length
     */
    public FecEncoder(int columns, int rows, Sink sink, ByteBufAllocator allocator) {
        validate(columns, rows);
        this.columns = columns;
        this.rows = rows;
        this.sink = sink;
        this.allocator = allocator;
        this.rowXor = new XorAccumulator(allocator);
        this.columnXor = new XorAccumulator[columns];
        this.columnBase = new int[columns];
        this.columnTimestamp = new long[columns];
        for (int c = 0; c < columns; c++) {
            columnXor[c] = new XorAccumulator(allocator);
        }
    }

    /** Folds one media packet in, emitting whatever FEC it completes or releases. Does not take the packet. */
    public void onMedia(RtpPacket packet) {
        int column = index % columns;
        int row = index / columns;

        if (column == 0) {
            rowBase = packet.sequenceNumber();
            rowTimestamp = packet.timestamp();
        }
        rowXor.add(packet);
        if (column == columns - 1) {
            sink.row(finish(rowXor, rowBase, rowTimestamp, true, ++rowSequence));
        }

        if (row == 0) {
            columnBase[column] = packet.sequenceNumber();
            columnTimestamp[column] = packet.timestamp();
        }
        columnXor[column].add(packet);

        // Last matrix's columns, one every D packets through this one.
        if (index % rows == 0 && !heldColumns.isEmpty()) {
            sink.column(heldColumns.poll());
        }

        index++;
        if (index == columns * rows) {
            index = 0;
            while (!heldColumns.isEmpty()) {
                sink.column(heldColumns.poll()); // only if L > D left some unsent
            }
            for (int c = 0; c < columns; c++) {
                heldColumns.add(finish(columnXor[c], columnBase[c], columnTimestamp[c], false, ++columnSequence));
            }
        }
    }

    /** Releases whatever is accumulated or held. */
    public void close() {
        rowXor.release();
        for (XorAccumulator column : columnXor) {
            column.release();
        }
        heldColumns.forEach(packet -> packet.payload().release());
        heldColumns.clear();
    }

    private FecPacket finish(XorAccumulator xor, int base, long timestamp, boolean row, int sequence) {
        return new FecPacket(sequence & 0xFFFF, timestamp, xor.flags(), xor.marker(), base & 0xFFFF,
                xor.length(), xor.payloadType(), xor.timestamp(), row, row ? 1 : columns,
                row ? columns : rows, xor.takeBody());
    }
}