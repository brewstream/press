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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * The RTCP packets Press reads and writes (RFC 3550 §6.4 to §6.6): sender and
 * receiver reports, the CNAME item of a source description, and BYE. Anything
 * else in a compound packet (APP, XR, feedback) is skipped over by its length.
 *
 * <p>SSRCs and the 32-bit report fields are unsigned values held in a
 * {@code long}.
 */
public sealed interface RtcpPacket {

    int TYPE_SR = 200;
    int TYPE_RR = 201;
    int TYPE_SDES = 202;
    int TYPE_BYE = 203;

    /** SDES item type for the canonical name (RFC 3550 §6.5.1). */
    int SDES_CNAME = 1;

    /**
     * One reception report (RFC 3550 §6.4.1).
     *
     * @param ssrc                  the source this block reports on
     * @param fractionLost          lost since the last report, as an 8-bit fixed-point fraction
     * @param cumulativeLost        lost since the start, 24-bit signed
     * @param extendedHighestSeq    cycles in the high 16 bits, highest sequence number in the low 16
     * @param jitter                interarrival jitter in timestamp units
     * @param lastSenderReport      middle 32 bits of the NTP timestamp of the last SR received (LSR),
     *                              0 if none
     * @param delaySinceLastSenderReport time since that SR in units of 1/65536 s (DLSR)
     */
    record ReportBlock(long ssrc, int fractionLost, int cumulativeLost, long extendedHighestSeq, long jitter,
            long lastSenderReport, long delaySinceLastSenderReport) {

        static final int LENGTH = 24;

        void encodeTo(ByteBuf out) {
            out.writeInt((int) ssrc);
            out.writeInt((fractionLost & 0xFF) << 24 | cumulativeLost & 0xFFFFFF);
            out.writeInt((int) extendedHighestSeq);
            out.writeInt((int) jitter);
            out.writeInt((int) lastSenderReport);
            out.writeInt((int) delaySinceLastSenderReport);
        }

        static ReportBlock decode(ByteBuf in, int at) {
            int lossWord = in.getInt(at + 4);
            return new ReportBlock(
                    in.getUnsignedInt(at),
                    lossWord >>> 24,
                    lossWord << 8 >> 8, // sign-extend the 24-bit field
                    in.getUnsignedInt(at + 8),
                    in.getUnsignedInt(at + 12),
                    in.getUnsignedInt(at + 16),
                    in.getUnsignedInt(at + 20));
        }
    }

    /**
     * SR (RFC 3550 §6.4.1).
     *
     * @param ntpTimestamp 64-bit NTP wall-clock time the report was sent
     * @param rtpTimestamp the same instant on the RTP clock
     */
    record SenderReport(long ssrc, long ntpTimestamp, long rtpTimestamp, long packetCount, long octetCount,
            List<ReportBlock> reports) implements RtcpPacket {

        public SenderReport {
            reports = List.copyOf(reports);
        }

        /** The middle 32 bits of the NTP timestamp, which a receiver echoes as LSR. */
        public long compactNtp() {
            return ntpTimestamp >>> 16 & 0xFFFF_FFFFL;
        }
    }

    /** RR (RFC 3550 §6.4.2). */
    record ReceiverReport(long ssrc, List<ReportBlock> reports) implements RtcpPacket {
        public ReceiverReport {
            reports = List.copyOf(reports);
        }
    }

    /** SDES carrying one CNAME for one source, which is all RFC 3550 requires (§6.5). */
    record SourceDescription(long ssrc, String cname) implements RtcpPacket {
    }

    /** BYE (RFC 3550 §6.6), without the optional reason. */
    record Goodbye(List<Long> ssrcs) implements RtcpPacket {
        public Goodbye {
            ssrcs = List.copyOf(ssrcs);
        }
    }

    /** Writes the packets one after another as a compound packet. */
    static void encodeCompound(List<? extends RtcpPacket> packets, ByteBuf out) {
        for (RtcpPacket packet : packets) {
            encode(packet, out);
        }
    }

    /** Writes one packet, header included. */
    static void encode(RtcpPacket packet, ByteBuf out) {
        int start = out.writerIndex();
        switch (packet) {
            case SenderReport sr -> {
                header(out, sr.reports().size(), TYPE_SR);
                out.writeInt((int) sr.ssrc());
                out.writeLong(sr.ntpTimestamp());
                out.writeInt((int) sr.rtpTimestamp());
                out.writeInt((int) sr.packetCount());
                out.writeInt((int) sr.octetCount());
                sr.reports().forEach(block -> block.encodeTo(out));
            }
            case ReceiverReport rr -> {
                header(out, rr.reports().size(), TYPE_RR);
                out.writeInt((int) rr.ssrc());
                rr.reports().forEach(block -> block.encodeTo(out));
            }
            case SourceDescription sdes -> {
                header(out, 1, TYPE_SDES);
                out.writeInt((int) sdes.ssrc());
                byte[] name = sdes.cname().getBytes(StandardCharsets.UTF_8);
                int length = Math.min(name.length, 255);
                out.writeByte(SDES_CNAME);
                out.writeByte(length);
                out.writeBytes(name, 0, length);
                // The item list ends with a zero byte, then pads to a 32-bit boundary (§6.5).
                int itemBytes = 2 + length + 1;
                out.writeZero(1 + (4 - itemBytes % 4) % 4);
            }
            case Goodbye bye -> {
                header(out, bye.ssrcs().size(), TYPE_BYE);
                bye.ssrcs().forEach(ssrc -> out.writeInt((int) (long) ssrc));
            }
        }
        int words = (out.writerIndex() - start) / 4 - 1;
        out.setShort(start + 2, words);
    }

    private static void header(ByteBuf out, int count, int type) {
        out.writeByte(0x80 | count & 0x1F);
        out.writeByte(type);
        out.writeShort(0); // length, filled in once the body is written
    }

    /**
     * Reads a compound packet, or returns {@code null} when it fails the checks
     * of RFC 3550 Appendix A.2: every packet version 2, and lengths that add up
     * to exactly the datagram. Packets of types Press does not use are skipped.
     * A.2 also wants the first packet to be SR or RR; that is not enforced,
     * because reduced-size RTCP (RFC 5506) legitimately breaks it.
     */
    static List<RtcpPacket> decodeCompound(ByteBuf in) {
        List<RtcpPacket> packets = new ArrayList<>();
        int at = in.readerIndex();
        int end = in.writerIndex();
        if (end - at < 4) {
            return null;
        }
        while (at < end) {
            if (end - at < 4) {
                return null;
            }
            int b0 = in.getUnsignedByte(at);
            if (b0 >> 6 != 2) {
                return null;
            }
            int count = b0 & 0x1F;
            int type = in.getUnsignedByte(at + 1);
            int length = (in.getUnsignedShort(at + 2) + 1) * 4;
            if (at + length > end) {
                return null;
            }
            boolean padded = (b0 & 0x20) != 0;
            int bodyEnd = at + length - (padded ? in.getUnsignedByte(at + length - 1) : 0);
            RtcpPacket packet = decodeOne(in, type, count, at + 4, bodyEnd);
            if (packet != null) {
                packets.add(packet);
            }
            at += length;
        }
        return packets;
    }

    private static RtcpPacket decodeOne(ByteBuf in, int type, int count, int at, int end) {
        switch (type) {
            case TYPE_SR -> {
                if (end - at < 24 + count * ReportBlock.LENGTH) {
                    return null;
                }
                return new SenderReport(in.getUnsignedInt(at), in.getLong(at + 4), in.getUnsignedInt(at + 12),
                        in.getUnsignedInt(at + 16), in.getUnsignedInt(at + 20), blocks(in, at + 24, count));
            }
            case TYPE_RR -> {
                if (end - at < 4 + count * ReportBlock.LENGTH) {
                    return null;
                }
                return new ReceiverReport(in.getUnsignedInt(at), blocks(in, at + 4, count));
            }
            case TYPE_SDES -> {
                return count == 0 || end - at < 4 ? null : cname(in, at, end);
            }
            case TYPE_BYE -> {
                if (end - at < 4 * count) {
                    return null;
                }
                List<Long> ssrcs = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    ssrcs.add(in.getUnsignedInt(at + 4 * i));
                }
                return new Goodbye(ssrcs);
            }
            default -> {
                return null;
            }
        }
    }

    private static List<ReportBlock> blocks(ByteBuf in, int at, int count) {
        List<ReportBlock> blocks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            blocks.add(ReportBlock.decode(in, at + i * ReportBlock.LENGTH));
        }
        return blocks;
    }

    /** The first chunk's CNAME, or {@code null} if it has none. */
    private static SourceDescription cname(ByteBuf in, int at, int end) {
        long ssrc = in.getUnsignedInt(at);
        int item = at + 4;
        while (item + 2 <= end) {
            int itemType = in.getUnsignedByte(item);
            if (itemType == 0) {
                return null;
            }
            int length = in.getUnsignedByte(item + 1);
            if (item + 2 + length > end) {
                return null;
            }
            if (itemType == SDES_CNAME) {
                return new SourceDescription(ssrc, in.toString(item + 2, length, StandardCharsets.UTF_8));
            }
            item += 2 + length;
        }
        return null;
    }
}