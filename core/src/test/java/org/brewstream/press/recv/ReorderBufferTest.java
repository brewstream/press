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

import io.netty.buffer.Unpooled;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReorderBufferTest {

    private static final long MS = 1_000_000;
    private static final long LATENCY = 100 * MS;

    private final List<Long> delivered = new ArrayList<>();
    private final List<String> lost = new ArrayList<>();
    private final List<RtpPacket> deliveredPackets = new ArrayList<>();
    private final ReorderBuffer buffer = new ReorderBuffer(64, LATENCY, new ReorderBuffer.Sink() {
        @Override
        public void deliver(long extendedSeq, RtpPacket packet) {
            delivered.add(extendedSeq);
            deliveredPackets.add(packet);
        }

        @Override
        public void lost(long firstExtendedSeq, int count) {
            lost.add(firstExtendedSeq + "+" + count);
        }
    });

    @Test
    void deliversInOrderPacketsImmediately() {
        for (long seq = 10; seq < 15; seq++) {
            assertThat(buffer.offer(seq, packet(), 0)).isEqualTo(ReorderBuffer.Offer.ACCEPTED);
        }

        assertThat(delivered).containsExactly(10L, 11L, 12L, 13L, 14L);
        assertThat(buffer.buffered()).isZero();
    }

    @Test
    void putsReorderedPacketsBackInSequence() {
        buffer.offer(1, packet(), 0);
        buffer.offer(3, packet(), MS);
        buffer.offer(4, packet(), 2 * MS);
        assertThat(delivered).containsExactly(1L);

        buffer.offer(2, packet(), 3 * MS);

        assertThat(delivered).containsExactly(1L, 2L, 3L, 4L);
        assertThat(lost).isEmpty();
    }

    @Test
    void givesUpOnAGapOnceTheOldestPacketBehindItHasWaitedTheLatency() {
        buffer.offer(1, packet(), 0);
        buffer.offer(3, packet(), 10 * MS);
        buffer.offer(4, packet(), 50 * MS);

        buffer.drain(10 * MS + LATENCY - 1);
        assertThat(delivered).containsExactly(1L);

        buffer.drain(10 * MS + LATENCY);
        assertThat(lost).containsExactly("2+1");
        assertThat(delivered).containsExactly(1L, 3L, 4L);
    }

    /**
     * The wait is counted from the oldest packet waiting, not from the one right
     * after the gap. After 2 times out, 4 is missing with 6 (arrived at 20 ms)
     * and 5 (arrived at 30 ms) behind it: 6 has waited longest and decides, so
     * 4 is given up at 120 ms, not 130.
     */
    @Test
    void countsTheWaitFromTheOldestArrivalNotTheLowestSequence() {
        buffer.offer(1, packet(), 0);
        buffer.offer(3, packet(), 10 * MS);
        buffer.offer(6, packet(), 20 * MS);
        buffer.offer(5, packet(), 30 * MS);

        buffer.drain(10 * MS + LATENCY);
        assertThat(delivered).containsExactly(1L, 3L);

        buffer.drain(20 * MS + LATENCY);
        assertThat(lost).containsExactly("2+1", "4+1");
        assertThat(delivered).containsExactly(1L, 3L, 5L, 6L);
    }

    @Test
    void timesOutConsecutiveGapsIndependently() {
        buffer.offer(1, packet(), 0);
        buffer.offer(3, packet(), 10 * MS);
        buffer.offer(6, packet(), 20 * MS);

        buffer.drain(10 * MS + LATENCY);
        // 3 was released; 6 has waited only 90 ms by now, so 4-5 still have time.
        assertThat(delivered).containsExactly(1L, 3L);
        assertThat(lost).containsExactly("2+1");

        buffer.drain(20 * MS + LATENCY);
        assertThat(delivered).containsExactly(1L, 3L, 6L);
        assertThat(lost).containsExactly("2+1", "4+2");
    }

    @Test
    void tellsADuplicateFromALatePacket() {
        buffer.offer(1, packet(), 0);
        buffer.offer(3, packet(), 0);
        buffer.drain(LATENCY);
        assertThat(lost).containsExactly("2+1");

        assertThat(buffer.offer(1, packet(), LATENCY)).isEqualTo(ReorderBuffer.Offer.DUPLICATE);
        assertThat(buffer.offer(2, packet(), LATENCY)).isEqualTo(ReorderBuffer.Offer.LATE);

        buffer.offer(5, packet(), LATENCY);
        assertThat(buffer.offer(5, packet(), LATENCY)).as("a duplicate still waiting in the buffer")
                .isEqualTo(ReorderBuffer.Offer.DUPLICATE);
    }

    @Test
    void aPacketBeyondTheWindowForcesDeliveryForward() {
        buffer.offer(0, packet(), 0);
        buffer.offer(2, packet(), 0);

        buffer.offer(100, packet(), MS); // window is 64

        assertThat(delivered).containsExactly(0L, 2L);
        assertThat(lost).containsExactly("1+1", "3+34");
        assertThat(buffer.nextSeq()).isEqualTo(37);
    }

    @Test
    void flushDeliversEverythingAndDeclaresEveryGapLost() {
        buffer.offer(1, packet(), 0);
        buffer.offer(4, packet(), 0);
        buffer.offer(6, packet(), 0);

        buffer.flush();

        assertThat(delivered).containsExactly(1L, 4L, 6L);
        assertThat(lost).containsExactly("2+2", "5+1");
        assertThat(buffer.buffered()).isZero();
    }

    @Test
    void resetReleasesWhatIsHeldAndStartsOver() {
        buffer.offer(1, packet(), 0);
        RtpPacket held = packet();
        buffer.offer(3, held, 0);

        buffer.reset();

        assertThat(held.body().refCnt()).isZero();
        assertThat(buffer.offer(9000, packet(), 0)).isEqualTo(ReorderBuffer.Offer.ACCEPTED);
        assertThat(delivered).containsExactly(1L, 9000L);
    }

    @Test
    void handsOwnershipToTheSink() {
        RtpPacket packet = packet();
        buffer.offer(1, packet, 0);

        assertThat(deliveredPackets).containsExactly(packet);
        assertThat(packet.body().refCnt()).as("delivered, not released").isEqualTo(1);
    }

    private static RtpPacket packet() {
        return RtpPacket.of(false, 33, 0, 0, 1, Unpooled.buffer(4).writeInt(0));
    }
}