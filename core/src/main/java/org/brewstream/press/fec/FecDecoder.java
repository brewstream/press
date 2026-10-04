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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Rebuilds lost media packets from SMPTE 2022-1 row and column FEC.
 *
 * <p>An FEC packet is the XOR of the packets it protects, so if exactly one of
 * them is missing, XORing the FEC packet with all the others gives the missing
 * one back. Recovery is attempted as soon as it can succeed: when an FEC packet
 * arrives, and again whenever a media packet it covers arrives or is
 * recovered. A packet recovered from a row can complete a column and the other
 * way round, so two-dimensional FEC repairs patterns neither dimension could
 * alone: a burst of up to L consecutive losses, through the columns, plus
 * scattered single losses, through the rows.
 *
 * <p>Each FEC packet says what it protects (base, offset, count), so the decoder
 * needs no configuration and learns L and D from what it sees. It keeps recent
 * media packets, including those already delivered, because a column FEC packet
 * arrives after its whole column, and with ffmpeg's pacing up to a full matrix
 * later than that. Packets older than the history window, and FEC packets that
 * only cover them, are dropped.
 *
 * <p>Not thread-safe; owned by one receiver's event loop.
 */
public final class FecDecoder {

    /** Where recovered packets go. The sink owns each one. */
    public interface Sink {
        void recovered(long extendedSeq, RtpPacket packet);
    }

    private final int capacity;
    private final int mask;
    private final Sink sink;
    private final ByteBufAllocator allocator;

    private final RtpPacket[] history;
    private final long[] historySeq;
    private boolean started;
    private long highestSeq;
    private final List<Pending> pending = new ArrayList<>();

    private int columns;
    private int rows;
    private long fecPackets;
    private long recovered;
    private long failed;

    private record Pending(FecPacket fec, long base) {
        long last() {
            return base + (long) fec.offset() * (fec.na() - 1);
        }

        boolean covers(long seq) {
            return seq >= base && seq <= last() && (seq - base) % fec.offset() == 0;
        }
    }

    /**
     * @param capacity media packets of history to keep, rounded up to a power of
     *                 two. Must cover the oldest packet a late column FEC packet can
     *                 protect: two matrices is enough for ffmpeg's pacing, so the
     *                 2022-1 maximum of 100 packets per matrix needs 200
     */
    public FecDecoder(int capacity, Sink sink, ByteBufAllocator allocator) {
        this.capacity = Integer.highestOneBit(Math.max(2, capacity - 1)) << 1;
        this.mask = this.capacity - 1;
        this.sink = sink;
        this.allocator = allocator;
        this.history = new RtpPacket[this.capacity];
        this.historySeq = new long[this.capacity];
        Arrays.fill(historySeq, Long.MIN_VALUE);
    }

    /** Records a media packet as received, and recovers anything it completes. Does not take the packet. */
    public void onMedia(long extendedSeq, RtpPacket packet) {
        if (has(extendedSeq)) {
            return;
        }
        remember(extendedSeq, new RtpPacket(packet.flags(), packet.marker(), packet.payloadType(),
                packet.sequenceNumber(), packet.timestamp(), packet.ssrc(), packet.body().retainedDuplicate()));
        settle(extendedSeq);
    }

    /** Takes an FEC packet (and its payload), recovering at once if it can. */
    public void onFec(FecPacket fec) {
        fecPackets++;
        if (fec.row()) {
            columns = fec.na();
        } else {
            columns = fec.offset();
            rows = fec.na();
        }
        if (!started) {
            fec.payload().release(); // no media yet to place it against
            return;
        }
        Pending candidate = new Pending(fec, extend(fec.snBase()));
        if (candidate.last() <= highestSeq - capacity) {
            fec.payload().release(); // covers only packets already forgotten
            return;
        }
        pending.add(candidate);
        ArrayDeque<Long> recoveredNow = new ArrayDeque<>();
        attempt(candidate, recoveredNow);
        drainRecovered(recoveredNow);
    }

    /** Forgets everything, for a new source. */
    public void reset() {
        for (int i = 0; i < capacity; i++) {
            if (history[i] != null) {
                history[i].body().release();
                history[i] = null;
            }
        }
        Arrays.fill(historySeq, Long.MIN_VALUE);
        pending.forEach(p -> p.fec().payload().release());
        pending.clear();
        started = false;
    }

    /** L as last seen in an FEC packet, or 0 before any. */
    public int columns() {
        return columns;
    }

    /** D as last seen in a column FEC packet, or 0 before any (or with row FEC only). */
    public int rows() {
        return rows;
    }

    public long fecPackets() {
        return fecPackets;
    }

    public long recovered() {
        return recovered;
    }

    /** FEC packets that yielded a packet inconsistent with themselves, and were dropped. */
    public long failed() {
        return failed;
    }

    // -------------------------------------------------------------------------

    /** Re-tries every pending FEC packet that covers {@code seq}, and whatever that recovers in turn. */
    private void settle(long seq) {
        ArrayDeque<Long> work = new ArrayDeque<>();
        work.add(seq);
        drainRecovered(work);
    }

    private void drainRecovered(ArrayDeque<Long> work) {
        while (!work.isEmpty()) {
            long seq = work.poll();
            // Copy: attempt() removes from the list as it goes.
            for (Pending candidate : new ArrayList<>(pending)) {
                if (candidate.covers(seq)) {
                    attempt(candidate, work);
                }
            }
        }
        expire();
    }

    /** Recovers through one FEC packet if exactly one of its packets is missing; drops it once it is spent. */
    private void attempt(Pending candidate, ArrayDeque<Long> recoveredNow) {
        FecPacket fec = candidate.fec();
        long missing = Long.MIN_VALUE;
        int missingCount = 0;
        for (long seq = candidate.base(); seq <= candidate.last(); seq += fec.offset()) {
            if (!has(seq)) {
                missing = seq;
                if (++missingCount > 1) {
                    return; // wait for more to arrive or be recovered
                }
            }
        }
        pending.remove(candidate);
        if (missingCount == 0) {
            fec.payload().release();
            return;
        }

        XorAccumulator xor = new XorAccumulator(allocator);
        xor.start(fec.flagsRecovery(), fec.markerRecovery(), fec.ptRecovery(), fec.tsRecovery(),
                fec.lengthRecovery(), fec.payload());
        long ssrc = 0;
        for (long seq = candidate.base(); seq <= candidate.last(); seq += fec.offset()) {
            if (seq != missing) {
                RtpPacket present = history[(int) seq & mask];
                xor.add(present);
                ssrc = present.ssrc();
            }
        }
        fec.payload().release();
        int length = xor.length();
        int flags = xor.flags();
        boolean marker = xor.marker();
        int payloadType = xor.payloadType();
        long timestamp = xor.timestamp();
        io.netty.buffer.ByteBuf body = xor.takeBody();
        if (length > body.readableBytes()) {
            failed++;
            body.release();
            return;
        }
        body.writerIndex(length);
        RtpPacket packet = new RtpPacket(flags, marker, payloadType, (int) missing & 0xFFFF, timestamp, ssrc, body);
        recovered++;
        remember(missing, new RtpPacket(packet.flags(), packet.marker(), packet.payloadType(),
                packet.sequenceNumber(), packet.timestamp(), packet.ssrc(), body.retainedDuplicate()));
        sink.recovered(missing, packet);
        recoveredNow.add(missing);
    }

    private void remember(long seq, RtpPacket packet) {
        int slot = (int) seq & mask;
        if (history[slot] != null) {
            history[slot].body().release();
        }
        history[slot] = packet;
        historySeq[slot] = seq;
        if (!started || seq > highestSeq) {
            highestSeq = seq;
            started = true;
        }
    }

    private boolean has(long seq) {
        int slot = (int) seq & mask;
        return historySeq[slot] == seq && history[slot] != null;
    }

    /** Drops pending FEC packets whose oldest protected packet has left the history window. */
    private void expire() {
        long oldest = highestSeq - capacity + 1;
        Iterator<Pending> it = pending.iterator();
        while (it.hasNext()) {
            Pending candidate = it.next();
            if (candidate.base() < oldest) {
                candidate.fec().payload().release();
                it.remove();
            }
        }
    }

    /** The extended sequence number nearest the newest packet seen whose low 16 bits are {@code low}. */
    private long extend(int low) {
        long candidate = (highestSeq & ~0xFFFFL) | low;
        if (candidate - highestSeq > 0x8000) {
            candidate -= 0x10000;
        } else if (highestSeq - candidate > 0x8000) {
            candidate += 0x10000;
        }
        return candidate;
    }
}