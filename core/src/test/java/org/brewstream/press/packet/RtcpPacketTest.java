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

package org.brewstream.press.packet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.press.packet.RtcpPacket.Goodbye;
import org.brewstream.press.packet.RtcpPacket.ReceiverReport;
import org.brewstream.press.packet.RtcpPacket.ReportBlock;
import org.brewstream.press.packet.RtcpPacket.SenderReport;
import org.brewstream.press.packet.RtcpPacket.SourceDescription;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RtcpPacketTest {

    private static final ReportBlock BLOCK = new ReportBlock(0xCAFEBABEL, 64, -5, 0x0001_0010L, 450,
            0x1234_5678L, 0x0001_8000L);

    @Test
    void roundTripsACompoundSenderReportAndSourceDescription() {
        SenderReport sr = new SenderReport(0x8000_0001L, 0xE6F0_0000_8000_0000L, 0xFFFF_0000L, 1000, 1_316_000,
                List.of(BLOCK));
        SourceDescription sdes = new SourceDescription(0x8000_0001L, "press-test");

        List<RtcpPacket> decoded = roundTrip(List.of(sr, sdes));

        assertThat(decoded).containsExactly(sr, sdes);
    }

    @Test
    void roundTripsAReceiverReportWithNegativeCumulativeLoss() {
        ReceiverReport rr = new ReceiverReport(42, List.of(BLOCK));

        List<RtcpPacket> decoded = roundTrip(List.of(rr));

        assertThat(decoded).containsExactly(rr);
        assertThat(((ReceiverReport) decoded.getFirst()).reports().getFirst().cumulativeLost()).isEqualTo(-5);
    }

    @Test
    void roundTripsGoodbye() {
        Goodbye bye = new Goodbye(List.of(1L, 0xFFFF_FFFFL));

        assertThat(roundTrip(List.of(new ReceiverReport(7, List.of()), bye)))
                .containsExactly(new ReceiverReport(7, List.of()), bye);
    }

    /**
     * An RR with one report block, laid out by hand from RFC 3550 §6.4.2: the
     * length field counts 32-bit words minus one, and the loss word packs the
     * fraction above the 24-bit cumulative count.
     */
    @Test
    void encodesAReceiverReportByteForByte() {
        ByteBuf out = Unpooled.buffer();
        RtcpPacket.encode(new ReceiverReport(0x01020304L,
                List.of(new ReportBlock(0x0A0B0C0DL, 0x40, 3, 0x0002_0005L, 9, 0x11223344L, 0x00010000L))), out);

        assertThat(ByteBufUtil.hexDump(out)).isEqualTo(
                "81c90007" + "01020304"
                        + "0a0b0c0d" + "40000003" + "00020005" + "00000009" + "11223344" + "00010000");
    }

    /** RFC 3550 §6.5: the item list ends in a zero byte and pads to a 32-bit boundary. */
    @Test
    void padsSourceDescriptionChunksToAWordBoundary() {
        for (String name : List.of("a", "ab", "abc", "abcd", "abcde")) {
            ByteBuf out = Unpooled.buffer();
            RtcpPacket.encode(new SourceDescription(1, name), out);

            assertThat(out.readableBytes() % 4).as(name).isZero();
            assertThat(out.getByte(out.writerIndex() - 1)).as(name).isZero();
            assertThat(RtcpPacket.decodeCompound(out)).as(name).containsExactly(new SourceDescription(1, name));
        }
    }

    @Test
    void skipsPacketTypesItDoesNotUse() {
        ByteBuf out = Unpooled.buffer();
        RtcpPacket.encode(new ReceiverReport(7, List.of()), out);
        out.writeBytes(new byte[]{(byte) 0x80, (byte) 204, 0, 2, 0, 0, 0, 1, 'n', 'a', 'm', 'e'}); // APP

        assertThat(RtcpPacket.decodeCompound(out)).containsExactly(new ReceiverReport(7, List.of()));
    }

    @Test
    void rejectsACompoundWhoseLengthsDoNotAddUp() {
        ByteBuf out = Unpooled.buffer();
        RtcpPacket.encode(new ReceiverReport(7, List.of(BLOCK)), out);
        ByteBuf truncated = out.slice(0, out.readableBytes() - 4);

        assertThat(RtcpPacket.decodeCompound(truncated)).isNull();
    }

    @Test
    void rejectsAnythingButVersionTwo() {
        ByteBuf out = Unpooled.buffer();
        RtcpPacket.encode(new ReceiverReport(7, List.of()), out);
        out.setByte(0, 0x41);

        assertThat(RtcpPacket.decodeCompound(out)).isNull();
    }

    @Test
    void compactNtpIsTheMiddleThirtyTwoBits() {
        SenderReport sr = new SenderReport(1, 0x1122_3344_5566_7788L, 0, 0, 0, List.of());

        assertThat(sr.compactNtp()).isEqualTo(0x3344_5566L);
    }

    private static List<RtcpPacket> roundTrip(List<RtcpPacket> packets) {
        ByteBuf out = Unpooled.buffer();
        RtcpPacket.encodeCompound(packets, out);
        return RtcpPacket.decodeCompound(out);
    }
}