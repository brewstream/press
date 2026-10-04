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

/**
 * Extends 16-bit RTP sequence numbers into a 64-bit count that never wraps, and
 * keeps the loss figures RTCP receiver reports carry. A port of RFC 3550
 * Appendix A.1 ({@code update_seq}) and A.3 (the loss computations), with two
 * deliberate differences:
 *
 * <ul>
 *   <li><b>No probation.</b> A.1 wants {@code MIN_SEQUENTIAL} (2) packets in
 *       sequence before a new source counts, and discards the first. Press
 *       receives one locked SSRC that has already passed the header checks, so
 *       the first packet starts the source and is delivered.</li>
 *   <li><b>Reordered packets get an extended number too.</b> A.1 only counts
 *       them. Here a packet up to {@link #MAX_MISORDER} behind the highest one
 *       seen is placed in the cycle it belongs to, because the reorder buffer
 *       needs its position.</li>
 * </ul>
 *
 * <p>Not thread-safe; owned by one receiver's event loop.
 */
public final class SequenceTracker {

    /** A.1's value: about a minute at 50 packets a second. */
    static final int MAX_DROPOUT = 3000;
    /** A.1's value: about two seconds at 50 packets a second. */
    static final int MAX_MISORDER = 100;

    private static final int SEQ_MOD = 1 << 16;

    /** Returned by {@link #update} for a packet that cannot be placed. */
    public static final long INVALID = -1;

    private boolean started;
    private long baseSeq;
    private int maxSeq;
    private long cycles;
    private int badSeq;
    private long received;
    private long receivedPrior;
    private long expectedPrior;
    private boolean restarted;

    /**
     * Records one arrival and returns its extended sequence number, or {@link
     * #INVALID} for a packet so far outside the expected range that it cannot be
     * placed (A.1 returns 0 for these). A second such packet that follows the
     * first in sequence is taken as the source having restarted: the counters
     * reset, {@link #restarted()} turns true, and numbering continues from there.
     */
    public long update(int seq) {
        restarted = false;
        if (!started) {
            start(seq);
            received++;
            return seq;
        }
        int udelta = (seq - maxSeq) & (SEQ_MOD - 1);
        long extended;
        if (udelta < MAX_DROPOUT) {
            if (seq < maxSeq) {
                cycles += SEQ_MOD;
            }
            maxSeq = seq;
            extended = cycles + seq;
        } else if (udelta <= SEQ_MOD - MAX_MISORDER) {
            if (seq != badSeq) {
                badSeq = (seq + 1) & (SEQ_MOD - 1);
                return INVALID;
            }
            start(seq);
            restarted = true;
            extended = seq;
        } else {
            // Behind maxSeq: a duplicate or a reordered packet. If it is
            // numerically above maxSeq, it is from before the last wrap.
            extended = seq > maxSeq ? cycles - SEQ_MOD + seq : cycles + seq;
        }
        received++;
        return extended;
    }

    /** Forgets the source, so the next packet starts a new one. */
    public void reset() {
        started = false;
        restarted = false;
    }

    /** Whether the last {@link #update} resynchronised on a restarted source. */
    public boolean restarted() {
        return restarted;
    }

    private void start(int seq) {
        started = true;
        baseSeq = seq;
        maxSeq = seq;
        badSeq = SEQ_MOD + 1;
        cycles = 0;
        received = 0;
        receivedPrior = 0;
        expectedPrior = 0;
    }

    /** The highest extended sequence number seen (A.3's {@code extended_max}). */
    public long extendedMax() {
        return cycles + maxSeq;
    }

    /** Packets expected since the source started: highest minus first, plus one. */
    public long expected() {
        return started ? extendedMax() - baseSeq + 1 : 0;
    }

    /** Arrivals counted, including duplicates, as A.1 counts them. */
    public long received() {
        return received;
    }

    /**
     * Cumulative packets lost (A.3): expected minus received, so duplicates can
     * make it negative. Clamped to the 24-bit signed range a report block holds.
     */
    public int cumulativeLost() {
        long lost = expected() - received;
        return (int) Math.max(-0x800000, Math.min(0x7FFFFF, lost));
    }

    /**
     * The fraction lost since the previous call, as the 8-bit fixed-point value
     * a report block carries (A.3). Starts a new interval, so call it once per
     * report.
     */
    public int takeFractionLost() {
        long expected = expected();
        long expectedInterval = expected - expectedPrior;
        expectedPrior = expected;
        long receivedInterval = received - receivedPrior;
        receivedPrior = received;
        long lostInterval = expectedInterval - receivedInterval;
        if (expectedInterval == 0 || lostInterval <= 0) {
            return 0;
        }
        return (int) ((lostInterval << 8) / expectedInterval);
    }
}