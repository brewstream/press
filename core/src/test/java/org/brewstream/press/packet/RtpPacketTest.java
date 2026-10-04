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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RtpPacketTest {

    @Test
    void roundTripsTheFixedHeaderWithUnsignedFields() {
        RtpPacket original = RtpPacket.of(true, 33, 0xFFFF, 0xFFFF_FFF0L, 0xDEAD_BEEFL,
                Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));

        ByteBuf wire = original.encode(ByteBufAllocator.DEFAULT);
        RtpPacket decoded = RtpPacket.decode(wire);

        assertThat(decoded).isNotNull();
        assertThat(decoded.marker()).isTrue();
        assertThat(decoded.payloadType()).isEqualTo(33);
        assertThat(decoded.sequenceNumber()).isEqualTo(0xFFFF);
        assertThat(decoded.timestamp()).isEqualTo(0xFFFF_FFF0L);
        assertThat(decoded.ssrc()).isEqualTo(0xDEAD_BEEFL);
        assertThat(ByteBufUtil.getBytes(decoded.payload())).containsExactly(1, 2, 3);
        decoded.body().release();
        wire.release();
    }

    /** The first byte and header layout are pinned against RFC 3550 §5.1 by hand. */
    @Test
    void encodesTheHeaderBitsWhereTheRfcPutsThem() {
        RtpPacket packet = RtpPacket.of(false, 33, 0x1234, 0x0A0B0C0DL, 0x01020304L, Unpooled.EMPTY_BUFFER);

        ByteBuf wire = packet.encode(ByteBufAllocator.DEFAULT);

        assertThat(ByteBufUtil.hexDump(wire)).isEqualTo("8021" + "1234" + "0a0b0c0d" + "01020304");
        wire.release();
    }

    @Test
    void payloadSkipsCsrcsExtensionAndPadding() {
        ByteBuf wire = Unpooled.buffer();
        wire.writeByte(0x80 | 0x20 | 0x10 | 2); // V=2, P, X, CC=2
        wire.writeByte(33);
        wire.writeShort(7);
        wire.writeInt(0);
        wire.writeInt(0);
        wire.writeInt(0x11111111); // CSRC 1
        wire.writeInt(0x22222222); // CSRC 2
        wire.writeShort(0xBEDE);   // extension profile
        wire.writeShort(1);        // one 32-bit word follows
        wire.writeInt(0x33333333);
        wire.writeBytes(new byte[]{9, 8, 7, 6, 5});
        wire.writeBytes(new byte[]{0, 0, 3}); // 3 bytes of padding

        RtpPacket packet = RtpPacket.decode(wire);

        assertThat(packet).isNotNull();
        assertThat(packet.csrcCount()).isEqualTo(2);
        assertThat(packet.extension()).isTrue();
        assertThat(packet.padding()).isTrue();
        assertThat(ByteBufUtil.getBytes(packet.payload())).containsExactly(9, 8, 7, 6, 5);
        // The body keeps everything after the fixed header, which is what FEC protects.
        assertThat(packet.body().readableBytes()).isEqualTo(8 + 8 + 5 + 3);
        packet.body().release();
        wire.release();
    }

    @Test
    void rejectsAnythingButVersionTwo() {
        assertThat(RtpPacket.decode(header(0x40, 33))).isNull();
        assertThat(RtpPacket.decode(header(0xC0, 33))).isNull();
    }

    /** RFC 3550 A.1: an RTCP SR or RR on the RTP port reads as PT 72 or 73 with the marker set. */
    @Test
    void rejectsRtcpSenderAndReceiverReports() {
        assertThat(RtpPacket.decode(header(0x80, 200))).isNull();
        assertThat(RtpPacket.decode(header(0x80, 201))).isNull();
    }

    @Test
    void rejectsLengthsThatDoNotFit() {
        assertThat(RtpPacket.decode(Unpooled.wrappedBuffer(new byte[11]))).isNull();
        // CC=3 promises 12 bytes of CSRCs that are not there.
        assertThat(RtpPacket.decode(header(0x83, 33))).isNull();
        // X set with no room for the extension header.
        assertThat(RtpPacket.decode(header(0x90, 33))).isNull();

        // P set, padding length larger than the body.
        ByteBuf padded = header(0xA0, 33);
        padded.writeBytes(new byte[]{1, 9});
        assertThat(RtpPacket.decode(padded)).isNull();
        // P set, padding length zero.
        ByteBuf zeroPadding = header(0xA0, 33);
        zeroPadding.writeBytes(new byte[]{1, 0});
        assertThat(RtpPacket.decode(zeroPadding)).isNull();
    }

    @Test
    void bodyIsARetainedSliceTheCallerReleasesSeparately() {
        ByteBuf wire = header(0x80, 33);
        wire.writeBytes(new byte[188]);

        RtpPacket packet = RtpPacket.decode(wire);
        wire.release();

        assertThat(packet.body().refCnt()).isPositive();
        assertThat(packet.payload().readableBytes()).isEqualTo(188);
        packet.body().release();
        assertThat(wire.refCnt()).isZero();
    }

    private static ByteBuf header(int firstByte, int secondByte) {
        ByteBuf wire = Unpooled.buffer();
        wire.writeByte(firstByte);
        wire.writeByte(secondByte);
        wire.writeShort(1);
        wire.writeInt(0);
        wire.writeInt(0);
        return wire;
    }
}