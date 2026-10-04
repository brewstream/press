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

/**
 * One SMPTE 2022-1 FEC packet: an RTP packet whose payload is a 16-byte FEC
 * header followed by the XOR of the media packets it protects.
 *
 * <pre>
 *  RTP header (12 bytes; P, X, CC and M carry the recovery of those bits)
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |      SNBase low bits          |        Length Recovery        |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |E| PT recovery |                    Mask                       |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                          TS recovery                          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |X|D|type |index|    Offset     |      NA       |SNBase ext bits|
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                  XOR of the protected bodies ...              |
 * </pre>
 *
 * <p>The packet protects {@code na} media packets: sequence numbers
 * {@code snBase}, {@code snBase + offset}, and so on. A row FEC packet ({@code
 * D = 1}) has offset 1 and protects one row of L packets. A column FEC packet
 * ({@code D = 0}) has offset L and protects one column of D packets. This is the
 * Pro-MPEG Code of Practice #3 layout that SMPTE 2022-1 adopted, built on
 * RFC 2733: the recovery fields are the XOR of the protected packets' fields,
 * and the length and payload cover everything after their fixed RTP header.
 * Only the XOR type (0) exists in 2022-1, and E is always 1.
 *
 * @param sequenceNumber the FEC stream's own RTP sequence number
 * @param timestamp      the FEC packet's RTP timestamp: the first protected packet's
 * @param flagsRecovery  XOR of the protected packets' P, X and CC bits
 * @param markerRecovery XOR of their marker bits
 * @param snBase         lowest protected sequence number, 16 bits
 * @param lengthRecovery XOR of their body lengths
 * @param ptRecovery     XOR of their payload types
 * @param tsRecovery     XOR of their timestamps
 * @param row            the D bit: a row packet rather than a column one
 * @param offset         the step between protected sequence numbers
 * @param na             how many packets it protects
 * @param payload        XOR of their bodies, each zero-padded to the longest
 */
public record FecPacket(
        int sequenceNumber,
        long timestamp,
        int flagsRecovery,
        boolean markerRecovery,
        int snBase,
        int lengthRecovery,
        int ptRecovery,
        long tsRecovery,
        boolean row,
        int offset,
        int na,
        ByteBuf payload) {

    /** The dynamic payload type senders conventionally give FEC streams. */
    public static final int PAYLOAD_TYPE = 96;

    /** RTP header plus FEC header. */
    public static final int HEADER_LENGTH = RtpPacket.HEADER_LENGTH + 16;

    /**
     * Reads one FEC packet, or returns {@code null} for one that is not RTP
     * version 2, is shorter than the two headers, uses an FEC type other than
     * XOR, or protects nothing. A row packet must step by 1.
     *
     * <p>The payload is a retained slice; the caller still owns {@code datagram}.
     */
    public static FecPacket decode(ByteBuf datagram) {
        int at = datagram.readerIndex();
        if (datagram.readableBytes() < HEADER_LENGTH || datagram.getUnsignedByte(at) >> 6 != 2) {
            return null;
        }
        int fec = at + RtpPacket.HEADER_LENGTH;
        int bits = datagram.getUnsignedByte(fec + 12);
        boolean row = (bits & 0x40) != 0;
        int type = bits >> 3 & 0x07;
        int offset = datagram.getUnsignedByte(fec + 13);
        int na = datagram.getUnsignedByte(fec + 14);
        if (type != 0 || offset == 0 || na == 0 || row && offset != 1) {
            return null;
        }
        return new FecPacket(
                datagram.getUnsignedShort(at + 2),
                datagram.getUnsignedInt(at + 4),
                datagram.getUnsignedByte(at) & 0x3F,
                (datagram.getUnsignedByte(at + 1) & 0x80) != 0,
                datagram.getUnsignedShort(fec),
                datagram.getUnsignedShort(fec + 2),
                datagram.getUnsignedByte(fec + 4) & 0x7F,
                datagram.getUnsignedInt(fec + 8),
                row,
                offset,
                na,
                datagram.retainedSlice(at + HEADER_LENGTH, datagram.readableBytes() - HEADER_LENGTH));
    }

    /** Writes the whole packet. SSRC is 0, as ffmpeg sends it. Does not release the payload. */
    public ByteBuf encode(ByteBufAllocator allocator) {
        ByteBuf out = allocator.buffer(HEADER_LENGTH + payload.readableBytes());
        out.writeByte(0x80 | flagsRecovery & 0x3F);
        out.writeByte((markerRecovery ? 0x80 : 0) | PAYLOAD_TYPE);
        out.writeShort(sequenceNumber);
        out.writeInt((int) timestamp);
        out.writeInt(0);
        out.writeShort(snBase);
        out.writeShort(lengthRecovery);
        out.writeByte(0x80 | ptRecovery & 0x7F); // E = 1
        out.writeMedium(0);                      // mask, unused by 2022-1
        out.writeInt((int) tsRecovery);
        out.writeByte(row ? 0x40 : 0);           // X = 0, D, type = 0 (XOR), index = 0
        out.writeByte(offset);
        out.writeByte(na);
        out.writeByte(0);                        // SNBase extension bits, unused
        out.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        return out;
    }
}