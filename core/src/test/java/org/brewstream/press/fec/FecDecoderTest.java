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
import io.netty.buffer.ByteBufUtil;
import org.brewstream.press.packet.FecPacket;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.brewstream.press.fec.FecCodecTest.media;

/**
 * Real encoder output through the decoder with packets removed. Each test lays
 * out the matrix as it is drawn in 2022-1: L columns, D rows, filled row by
 * row, so packet {@code r * L + c} is row r, column c.
 */
class FecDecoderTest {

    private static final int L = 4;
    private static final int D = 5;
    private static final int MATRIX = L * D;

    private final Map<Long, RtpPacket> recovered = new HashMap<>();
    private final FecDecoder decoder = new FecDecoder(256, (seq, packet) -> recovered.put(seq, packet),
            ByteBufAllocator.DEFAULT);

    @Test
    void rebuildsASingleLossFromItsRow() {
        run(0, Set.of(6L), true, false);

        assertRecovered(6);
    }

    @Test
    void rebuildsABurstAsLongAsARowFromTheColumns() {
        run(0, Set.of(4L, 5L, 6L, 7L), false, true);

        assertRecovered(4, 5, 6, 7);
    }

    /**
     * Rows 1 and 2 each lose two packets, which rows alone cannot fix, and
     * columns 0 and 1 each lose two, which columns alone cannot fix:
     *
     * <pre>
     *   row 1:  X X . .        packets 4, 5
     *   row 2:  . X X .        packets 9, 10
     *   row 3:  X . . .        packet 12
     * </pre>
     *
     * Column 2 repairs 10, which leaves row 2 with one loss, which repairs 9,
     * which leaves column 1 with one loss, which repairs 5, which leaves row 1
     * with one, which repairs 4, which leaves column 0 with one, which repairs 12.
     */
    @Test
    void rowsAndColumnsUnlockEachOther() {
        run(0, Set.of(4L, 5L, 9L, 10L, 12L), true, true);

        assertRecovered(4, 5, 9, 10, 12);
    }

    /** Two losses in each of two rows and two columns form a square no XOR can open. */
    @Test
    void cannotRebuildASquare() {
        run(0, Set.of(5L, 6L, 9L, 10L), true, true);

        assertThat(recovered).isEmpty();
        assertThat(decoder.failed()).isZero();
    }

    @Test
    void rebuildsAcrossTheSixteenBitSequenceWrap() {
        long start = 65536 - 2 * L; // the first matrix straddles the wrap
        run(start, Set.of(start + 9, start + 13), true, true);

        assertRecovered(start + 9, start + 13);
        assertThat(recovered.get(start + 9).sequenceNumber()).isEqualTo((int) ((start + 9) & 0xFFFF));
    }

    @Test
    void learnsTheMatrixFromTheFecPackets() {
        run(0, Set.of(), true, true);

        assertThat(decoder.columns()).isEqualTo(L);
        assertThat(decoder.rows()).isEqualTo(D);
    }

    @Test
    void reportsNothingWhenNothingIsMissing() {
        run(0, Set.of(), true, true);

        assertThat(recovered).isEmpty();
        assertThat(decoder.fecPackets()).isPositive();
    }

    /**
     * Runs two full matrices and a third (so the second's column FEC arrives),
     * in the order a sender emits them: media, with row FEC after each row and
     * column FEC spread through the next matrix.
     */
    private void run(long firstSeq, Set<Long> lost, boolean rows, boolean columns) {
        List<Object> wire = new ArrayList<>();
        FecEncoder encoder = new FecEncoder(L, D, new FecEncoder.Sink() {
            @Override
            public void column(FecPacket packet) {
                wire.add(packet);
            }

            @Override
            public void row(FecPacket packet) {
                wire.add(packet);
            }
        }, ByteBufAllocator.DEFAULT);
        Map<Long, RtpPacket> sent = new HashMap<>();
        for (long seq = firstSeq; seq < firstSeq + 3 * MATRIX; seq++) {
            RtpPacket packet = media((int) seq, 100 + (int) (seq % 7));
            sent.put(seq, packet);
            wire.add(seq);
            encoder.onMedia(packet);
        }
        encoder.close();

        for (Object item : wire) {
            if (item instanceof Long seq) {
                if (!lost.contains(seq)) {
                    decoder.onMedia(seq, sent.get(seq));
                }
            } else {
                FecPacket fec = (FecPacket) item;
                if (fec.row() ? rows : columns) {
                    decoder.onFec(fec);
                } else {
                    fec.payload().release();
                }
            }
        }
        this.sent = sent;
    }

    private Map<Long, RtpPacket> sent;

    private void assertRecovered(long... seqs) {
        assertThat(recovered.keySet()).containsExactlyInAnyOrder(java.util.Arrays.stream(seqs).boxed()
                .toArray(Long[]::new));
        for (long seq : seqs) {
            RtpPacket original = sent.get(seq);
            RtpPacket rebuilt = recovered.get(seq);
            assertThat(ByteBufUtil.getBytes(rebuilt.body())).as("body of " + seq)
                    .containsExactly(ByteBufUtil.getBytes(original.body()));
            assertThat(rebuilt.timestamp()).isEqualTo(original.timestamp());
            assertThat(rebuilt.payloadType()).isEqualTo(original.payloadType());
            assertThat(rebuilt.flags()).isEqualTo(original.flags());
            assertThat(rebuilt.marker()).isEqualTo(original.marker());
            assertThat(rebuilt.ssrc()).isEqualTo(original.ssrc());
        }
        assertThat(decoder.recovered()).isEqualTo(seqs.length);
    }
}