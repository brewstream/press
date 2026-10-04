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

class SequenceTrackerTest {

    @Test
    void theFirstPacketStartsTheSourceWithoutProbation() {
        SequenceTracker tracker = new SequenceTracker();

        assertThat(tracker.update(500)).isEqualTo(500);
        assertThat(tracker.expected()).isEqualTo(1);
        assertThat(tracker.cumulativeLost()).isZero();
    }

    @Test
    void extendsPastTheSixteenBitWrap() {
        SequenceTracker tracker = new SequenceTracker();

        assertThat(tracker.update(65534)).isEqualTo(65534);
        assertThat(tracker.update(65535)).isEqualTo(65535);
        assertThat(tracker.update(0)).isEqualTo(65536);
        assertThat(tracker.update(1)).isEqualTo(65537);
        assertThat(tracker.extendedMax()).isEqualTo(65537);
    }

    /** A packet from before the wrap that arrives after it belongs to the earlier cycle. */
    @Test
    void placesAPacketReorderedAcrossTheWrapInItsOwnCycle() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.update(65533);
        tracker.update(65534);
        tracker.update(0);

        assertThat(tracker.update(65535)).isEqualTo(65535);
        assertThat(tracker.update(1)).isEqualTo(65537);
    }

    @Test
    void countsLossAsExpectedMinusReceived() {
        SequenceTracker tracker = new SequenceTracker();
        for (int seq : new int[]{10, 11, 14, 15}) {
            tracker.update(seq);
        }

        assertThat(tracker.expected()).isEqualTo(6);
        assertThat(tracker.cumulativeLost()).isEqualTo(2);
    }

    /** RFC 3550 A.3: duplicates count as received, so they can push loss below zero. */
    @Test
    void duplicatesCanMakeCumulativeLossNegative() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.update(1);
        tracker.update(2);
        tracker.update(2);

        assertThat(tracker.cumulativeLost()).isEqualTo(-1);
    }

    @Test
    void fractionLostCoversOnlyTheIntervalSinceTheLastReport() {
        SequenceTracker tracker = new SequenceTracker();
        for (int seq = 0; seq < 4; seq++) {
            tracker.update(seq);
        }
        tracker.update(7); // 4, 5, 6 lost: 3 of 8 expected

        assertThat(tracker.takeFractionLost()).isEqualTo(3 * 256 / 8);

        tracker.update(8);
        tracker.update(9);
        assertThat(tracker.takeFractionLost()).as("nothing lost since the last report").isZero();
    }

    /**
     * RFC 3550 A.1: one packet far out of range is discarded, but a second that
     * follows it in sequence means the sender restarted, and counting starts over.
     */
    @Test
    void resynchronisesWhenTwoSequentialPacketsJumpFarAway() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.update(100);
        tracker.update(101);

        assertThat(tracker.update(40_000)).isEqualTo(SequenceTracker.INVALID);
        assertThat(tracker.restarted()).isFalse();

        assertThat(tracker.update(40_001)).isEqualTo(40_001);
        assertThat(tracker.restarted()).isTrue();
        assertThat(tracker.expected()).isEqualTo(1);
        assertThat(tracker.cumulativeLost()).isZero();
    }

    @Test
    void aSingleStrayPacketDoesNotDisturbTheStream() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.update(100);
        tracker.update(30_000);

        assertThat(tracker.update(101)).isEqualTo(101);
        assertThat(tracker.restarted()).isFalse();
    }

    @Test
    void aGapUnderMaxDropoutIsLossNotARestart() {
        SequenceTracker tracker = new SequenceTracker();
        tracker.update(100);

        assertThat(tracker.update(100 + SequenceTracker.MAX_DROPOUT - 1))
                .isEqualTo(100 + SequenceTracker.MAX_DROPOUT - 1);
        assertThat(tracker.restarted()).isFalse();
    }
}