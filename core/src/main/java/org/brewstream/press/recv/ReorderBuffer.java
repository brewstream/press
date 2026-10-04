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

package org.brewstream.press.recv;

import org.brewstream.press.packet.RtpPacket;

/**
 * Puts packets back in sequence order, and decides when a missing one is lost.
 *
 * <p>A packet that arrives in order is delivered at once, so a clean stream pays
 * no delay. When one is missing, the packets behind it wait, but only until the
 * oldest of them has waited {@code latency}. Then the missing packets are
 * declared lost, the ones behind them are delivered, and delivery continues.
 * That bound is what the receiver's configured latency means: no packet is held
 * longer than {@code latency} for the sake of an earlier one.
 *
 * <p>The window is a ring of {@code capacity} slots indexed by extended sequence
 * number. A packet so far ahead that it does not fit forces delivery forward
 * until it does, treating what is still missing as lost. A long outage cannot
 * grow the buffer without bound.
 *
 * <p>Not thread-safe; owned by one receiver's event loop. Packets handed to
 * {@link #offer} belong to this buffer until delivered to the sink, which then
 * owns them. A packet refused by {@code offer} still belongs to the caller.
 */
public final class ReorderBuffer {

    /** Where packets leave the buffer. */
    public interface Sink {
        /** A packet in sequence order. The sink owns it from here. */
        void deliver(long extendedSeq, RtpPacket packet);

        /** {@code count} consecutive packets from {@code firstExtendedSeq} will never be delivered. */
        void lost(long firstExtendedSeq, int count);
    }

    /** What {@link #offer} did with a packet. */
    public enum Offer {
        /** Buffered or delivered; the buffer owns it now. */
        ACCEPTED,
        /** Already delivered or already buffered. The caller still owns it. */
        DUPLICATE,
        /** Arrived after its slot was given up as lost. The caller still owns it. */
        LATE
    }

    private final int capacity;
    private final int mask;
    private final long latencyNanos;
    private final Sink sink;

    private final RtpPacket[] slots;
    private final long[] slotSeq;
    private final long[] slotArrival;
    /** Recently delivered sequence numbers, to tell a duplicate from a late packet. */
    private final long[] deliveredSeq;

    private boolean started;
    private long nextSeq;
    private long highestSeq;
    private int buffered;
    private boolean blocked;
    private long blockedSince;

    /**
     * @param capacity     window size in packets, rounded up to a power of two
     * @param latencyNanos how long the oldest packet behind a gap may wait
     */
    public ReorderBuffer(int capacity, long latencyNanos, Sink sink) {
        this.capacity = Integer.highestOneBit(Math.max(2, capacity - 1)) << 1;
        this.mask = this.capacity - 1;
        this.latencyNanos = latencyNanos;
        this.sink = sink;
        this.slots = new RtpPacket[this.capacity];
        this.slotSeq = new long[this.capacity];
        this.slotArrival = new long[this.capacity];
        this.deliveredSeq = new long[this.capacity];
        java.util.Arrays.fill(deliveredSeq, Long.MIN_VALUE);
    }

    /**
     * Takes one packet, delivering it and anything it unblocks straight away if
     * it is the next one due. Call {@link #drain} periodically as well, so gaps
     * time out even when nothing new arrives.
     */
    public Offer offer(long extendedSeq, RtpPacket packet, long nowNanos) {
        if (!started) {
            started = true;
            nextSeq = extendedSeq;
            highestSeq = extendedSeq - 1;
        }
        if (extendedSeq < nextSeq) {
            return deliveredSeq[(int) extendedSeq & mask] == extendedSeq ? Offer.DUPLICATE : Offer.LATE;
        }
        int slot = (int) extendedSeq & mask;
        if (slots[slot] != null && slotSeq[slot] == extendedSeq) {
            return Offer.DUPLICATE;
        }
        if (extendedSeq - nextSeq >= capacity) {
            advanceTo(extendedSeq - capacity + 1);
        }
        slots[slot] = packet;
        slotSeq[slot] = extendedSeq;
        slotArrival[slot] = nowNanos;
        buffered++;
        if (extendedSeq > highestSeq) {
            highestSeq = extendedSeq;
        }
        drain(nowNanos);
        return Offer.ACCEPTED;
    }

    /** Delivers whatever is due: everything in sequence, and anything a timed-out gap was holding. */
    public void drain(long nowNanos) {
        while (buffered > 0) {
            if (deliverRun()) {
                continue;
            }
            if (!blocked) {
                blocked = true;
                blockedSince = oldestArrival();
            }
            if (nowNanos - blockedSince < latencyNanos) {
                return;
            }
            skipToNextBuffered();
        }
    }

    /** Delivers everything buffered at once, declaring every gap lost. For a restart or a close. */
    public void flush() {
        while (buffered > 0) {
            if (!deliverRun()) {
                skipToNextBuffered();
            }
        }
    }

    /**
     * Forgets the stream's position, for a source that restarted with new
     * sequence numbers. Flush first; anything still buffered is released.
     */
    public void reset() {
        for (int i = 0; i < capacity; i++) {
            if (slots[i] != null) {
                slots[i].body().release();
                slots[i] = null;
            }
        }
        java.util.Arrays.fill(deliveredSeq, Long.MIN_VALUE);
        buffered = 0;
        blocked = false;
        started = false;
    }

    /** Packets currently held. */
    public int buffered() {
        return buffered;
    }

    /** The extended sequence number delivery is waiting for. */
    public long nextSeq() {
        return nextSeq;
    }

    /** Delivers the run starting at {@link #nextSeq}; false if that packet is missing. */
    private boolean deliverRun() {
        boolean any = false;
        while (true) {
            int slot = (int) nextSeq & mask;
            RtpPacket packet = slots[slot];
            if (packet == null || slotSeq[slot] != nextSeq) {
                return any;
            }
            slots[slot] = null;
            buffered--;
            blocked = false;
            deliveredSeq[slot] = nextSeq;
            sink.deliver(nextSeq, packet);
            nextSeq++;
            any = true;
        }
    }

    /** Declares the gap at {@link #nextSeq} lost, up to the next buffered packet. */
    private void skipToNextBuffered() {
        long first = nextSeq;
        while (slots[(int) nextSeq & mask] == null || slotSeq[(int) nextSeq & mask] != nextSeq) {
            nextSeq++;
        }
        blocked = false;
        reportLost(first, nextSeq - first);
    }

    /** Moves delivery forward to {@code target}, delivering or losing everything before it. */
    private void advanceTo(long target) {
        while (nextSeq < target) {
            if (!deliverRun()) {
                long first = nextSeq;
                while (nextSeq < target
                        && (slots[(int) nextSeq & mask] == null || slotSeq[(int) nextSeq & mask] != nextSeq)) {
                    nextSeq++;
                }
                blocked = false;
                reportLost(first, nextSeq - first);
            }
        }
    }

    private void reportLost(long first, long count) {
        while (count > 0) {
            int chunk = (int) Math.min(count, Integer.MAX_VALUE);
            sink.lost(first, chunk);
            first += chunk;
            count -= chunk;
        }
    }

    /** The earliest arrival among buffered packets. Only runs when a gap first blocks delivery. */
    private long oldestArrival() {
        long oldest = Long.MAX_VALUE;
        for (long seq = nextSeq; seq <= highestSeq; seq++) {
            int slot = (int) seq & mask;
            if (slots[slot] != null && slotSeq[slot] == seq && slotArrival[slot] < oldest) {
                oldest = slotArrival[slot];
            }
        }
        return oldest;
    }
}