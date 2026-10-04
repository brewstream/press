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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JitterEstimatorTest {

    private static final long CLOCK = 90_000;
    /** One packet every 10 ms: 900 ticks of a 90 kHz clock. */
    private static final long PERIOD_NANOS = 10_000_000;
    private static final long PERIOD_TICKS = 900;

    @Test
    void packetsArrivingExactlyOnTimeHaveNoJitter() {
        JitterEstimator jitter = new JitterEstimator(CLOCK);
        for (int i = 0; i < 100; i++) {
            jitter.update(1_000_000_000L + i * PERIOD_NANOS, 12_345 + i * PERIOD_TICKS);
        }

        assertThat(jitter.jitter()).isZero();
    }

    /**
     * Arrivals alternating 1 ms early and 1 ms late give |D| = 2 ms = 180 ticks
     * on every packet, and the 1/16 smoothing converges on exactly that.
     */
    @Test
    void convergesOnTheMeanDeviationOfTransitTime() {
        JitterEstimator jitter = new JitterEstimator(CLOCK);
        for (int i = 0; i < 1000; i++) {
            long wobble = i % 2 == 0 ? -1_000_000 : 1_000_000;
            jitter.update(5_000_000_000L + i * PERIOD_NANOS + wobble, i * PERIOD_TICKS);
        }

        assertThat(jitter.jitter()).isBetween(178L, 180L);
        assertThat(jitter.jitterMicros()).isBetween(1_970L, 2_000L);
    }

    /** RTP timestamps wrap at 32 bits; the transit difference must not jump when they do. */
    @Test
    void ignoresTheThirtyTwoBitTimestampWrap() {
        JitterEstimator jitter = new JitterEstimator(CLOCK);
        long start = 0xFFFF_FFFFL - 10 * PERIOD_TICKS;
        for (int i = 0; i < 40; i++) {
            jitter.update(i * PERIOD_NANOS, (start + i * PERIOD_TICKS) & 0xFFFF_FFFFL);
        }

        assertThat(jitter.jitter()).isZero();
    }
}