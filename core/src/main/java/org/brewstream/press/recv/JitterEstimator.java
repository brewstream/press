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
 * Interarrival jitter, RFC 3550 §6.4.1 and Appendix A.8: the smoothed mean
 * deviation of the difference in transit time between consecutive packets,
 * in RTP timestamp units. Uses A.8's integer form, which keeps the estimate
 * scaled by 16 to avoid floating point:
 *
 * <pre>
 *   jitter += |D| - ((jitter + 8) &gt;&gt; 4)      reported as jitter &gt;&gt; 4
 * </pre>
 *
 * <p>Arrival time is converted to the stream's clock (90 kHz for MPEG-TS), so
 * transit carries an arbitrary constant offset. Jitter only uses differences
 * between transit times, so the offset cancels.
 */
public final class JitterEstimator {

    private final long clockRate;
    private boolean primed;
    private long baseNanos;
    private long lastTransit;
    private long scaledJitter;

    public JitterEstimator(long clockRate) {
        this.clockRate = clockRate;
    }

    /**
     * @param arrivalNanos monotonic arrival time, as from {@link System#nanoTime()}
     * @param rtpTimestamp the packet's 32-bit RTP timestamp
     */
    public void update(long arrivalNanos, long rtpTimestamp) {
        if (!primed) {
            baseNanos = arrivalNanos;
        }
        // Measured from the first arrival so the multiplication cannot overflow.
        long arrival = (arrivalNanos - baseNanos) / 1_000L * clockRate / 1_000_000L;
        // Both sides wrap at 32 bits; the difference is taken in that space.
        long transit = (int) (arrival - rtpTimestamp);
        if (primed) {
            long d = Math.abs((int) (transit - lastTransit));
            scaledJitter += d - ((scaledJitter + 8) >> 4);
        }
        lastTransit = transit;
        primed = true;
    }

    /** Starts over, for a new source. */
    public void reset() {
        primed = false;
        scaledJitter = 0;
    }

    /** The current estimate in timestamp units, as a report block carries it. */
    public long jitter() {
        return scaledJitter >> 4;
    }

    /** The current estimate in microseconds. */
    public long jitterMicros() {
        return jitter() * 1_000_000L / clockRate;
    }
}