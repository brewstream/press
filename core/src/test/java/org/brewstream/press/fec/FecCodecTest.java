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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.press.packet.FecPacket;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The FEC packet layout, and what the encoder puts in it. */
class FecCodecTest {

    /**
     * Header bytes laid out by hand from the Pro-MPEG / SMPTE 2022-1 diagram,
     * matching what ffmpeg's prompeg.c writes: E set above the PT recovery,
     * mask zero, D in bit 6 of byte 24, offset and NA after it.
     */
    @Test
    void encodesTheFecHeaderWhereTheLayoutPutsIt() {
        FecPacket column = new FecPacket(0x0102, 0x0A0B0C0DL, 0x21, true, 0x1234, 0x0524, 33, 0x11223344L,
                false, 5, 4, Unpooled.wrappedBuffer(new byte[]{9}));

        ByteBuf wire = column.encode(ByteBufAllocator.DEFAULT);

        assertThat(ByteBufUtil.hexDump(wire)).isEqualTo(
                "a1" + "e0" + "0102" + "0a0b0c0d" + "00000000"        // RTP: P/X/CC recovery, M + PT 96
                        + "1234" + "0524" + "a1" + "000000" + "11223344" // SNBase, length, E|PT, mask, TS
                        + "00" + "05" + "04" + "00"                       // X D type index, offset, NA, ext
                        + "09");
        wire.release();
    }

    @Test
    void roundTripsRowAndColumnPackets() {
        for (boolean row : new boolean[]{true, false}) {
            FecPacket original = new FecPacket(7, 900, 0, false, 65535, 1316, 33, 12345, row, row ? 1 : 10,
                    row ? 10 : 5, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
            ByteBuf wire = original.encode(ByteBufAllocator.DEFAULT);

            FecPacket decoded = FecPacket.decode(wire);

            assertThat(decoded).usingRecursiveComparison().ignoringFields("payload").isEqualTo(original);
            assertThat(ByteBufUtil.getBytes(decoded.payload())).containsExactly(1, 2, 3);
            decoded.payload().release();
            wire.release();
        }
    }

    @Test
    void rejectsWhatItCannotUse() {
        FecPacket row = new FecPacket(1, 0, 0, false, 1, 0, 0, 0, true, 1, 5, Unpooled.EMPTY_BUFFER);
        ByteBuf wire = row.encode(ByteBufAllocator.DEFAULT);

        ByteBuf otherType = wire.copy();
        otherType.setByte(24, 0x40 | 1 << 3); // type 1
        assertThat(FecPacket.decode(otherType)).isNull();

        ByteBuf rowWithOffset = wire.copy();
        rowWithOffset.setByte(25, 5);
        assertThat(FecPacket.decode(rowWithOffset)).isNull();

        ByteBuf protectsNothing = wire.copy();
        protectsNothing.setByte(26, 0);
        assertThat(FecPacket.decode(protectsNothing)).isNull();

        assertThat(FecPacket.decode(wire.slice(0, 27))).isNull();
    }

    @Test
    void emitsOneRowPacketPerRowAndOneColumnPacketPerColumn() {
        List<FecPacket> rows = new ArrayList<>();
        List<FecPacket> columns = new ArrayList<>();
        FecEncoder encoder = encoder(4, 5, rows, columns);

        for (int seq = 100; seq < 100 + 3 * 20; seq++) {
            encoder.onMedia(media(seq, 188));
        }

        assertThat(rows).hasSize(15);
        assertThat(rows.getFirst().snBase()).isEqualTo(100);
        assertThat(rows.getFirst().offset()).isEqualTo(1);
        assertThat(rows.getFirst().na()).isEqualTo(4);
        assertThat(rows.get(1).snBase()).isEqualTo(104);

        // Matrices 1 and 2 are complete; matrix 1's columns went out during matrix 2,
        // matrix 2's during matrix 3. Matrix 3's are held for a matrix 4 that never came.
        assertThat(columns).extracting(FecPacket::snBase)
                .containsExactly(100, 101, 102, 103, 120, 121, 122, 123);
        assertThat(columns.getFirst().offset()).isEqualTo(4);
        assertThat(columns.getFirst().na()).isEqualTo(5);
        encoder.close();
    }

    /** Column packets are spread one every D media packets, not sent in a burst. */
    @Test
    void spreadsAMatrixsColumnPacketsOverTheNextMatrix() {
        List<String> order = new ArrayList<>();
        FecEncoder encoder = new FecEncoder(4, 5, new FecEncoder.Sink() {
            @Override
            public void column(FecPacket packet) {
                order.add("C" + packet.snBase());
                packet.payload().release();
            }

            @Override
            public void row(FecPacket packet) {
                packet.payload().release();
            }
        }, ByteBufAllocator.DEFAULT);

        for (int seq = 0; seq < 40; seq++) {
            order.add("M" + seq);
            encoder.onMedia(media(seq, 188));
        }

        assertThat(order.indexOf("C0")).isEqualTo(order.indexOf("M20") + 1);
        assertThat(order.indexOf("C1")).isEqualTo(order.indexOf("M25") + 1);
        assertThat(order.indexOf("C3")).isEqualTo(order.indexOf("M35") + 1);
        encoder.close();
    }

    @Test
    void theRowPacketIsTheXorOfItsRow() {
        List<FecPacket> rows = new ArrayList<>();
        FecEncoder encoder = encoder(4, 4, rows, new ArrayList<>());
        List<RtpPacket> row = List.of(media(0, 10), media(1, 6), media(2, 10), media(3, 10));

        row.forEach(encoder::onMedia);

        FecPacket fec = rows.getFirst();
        byte[] expected = new byte[10];
        for (RtpPacket packet : row) {
            byte[] body = ByteBufUtil.getBytes(packet.body());
            for (int i = 0; i < body.length; i++) {
                expected[i] ^= body[i];
            }
        }
        assertThat(ByteBufUtil.getBytes(fec.payload())).containsExactly(expected);
        assertThat(fec.lengthRecovery()).isEqualTo(10 ^ 6 ^ 10 ^ 10);
        assertThat(fec.tsRecovery()).isEqualTo(0L ^ 90 ^ 180 ^ 270);
        assertThat(fec.ptRecovery()).isEqualTo(0); // four 33s cancel
        encoder.close();
    }

    @Test
    void refusesMatricesOutsideTheStandardsLimits() {
        assertThatThrownBy(() -> FecEncoder.validate(21, 4)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FecEncoder.validate(5, 3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FecEncoder.validate(11, 10)).isInstanceOf(IllegalArgumentException.class);
        FecEncoder.validate(10, 10);
        FecEncoder.validate(1, 4);
    }

    static FecEncoder encoder(int columns, int rows, List<FecPacket> rowOut, List<FecPacket> columnOut) {
        return new FecEncoder(columns, rows, new FecEncoder.Sink() {
            @Override
            public void column(FecPacket packet) {
                columnOut.add(packet);
            }

            @Override
            public void row(FecPacket packet) {
                rowOut.add(packet);
            }
        }, ByteBufAllocator.DEFAULT);
    }

    /** A packet whose body bytes are derived from its sequence number, so every body differs. */
    static RtpPacket media(int seq, int length) {
        ByteBuf body = Unpooled.buffer(length);
        for (int i = 0; i < length; i++) {
            body.writeByte(seq * 31 + i * 7);
        }
        return RtpPacket.of(false, 33, seq & 0xFFFF, seq * 90L, 0xABCDL, body);
    }
}