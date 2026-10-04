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
 * One RTP data packet (RFC 3550 §5.1).
 *
 * <pre>
 *  0                   1                   2                   3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |V=2|P|X|  CC   |M|     PT      |       sequence number         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                           timestamp                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |           synchronization source (SSRC) identifier            |
 * +=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+
 * |   CSRC list, header extension, payload, padding  ...          |
 * </pre>
 *
 * <p>{@code body} is everything after the 12-byte fixed header, kept whole
 * rather than split into its parts, because that is the unit SMPTE 2022-1 FEC
 * protects: a recovered packet is rebuilt from the fixed-header fields and the
 * body, and the body's internal structure only matters to {@link #payload()}.
 * The P, X and CC bits stay in {@code flags} for the same reason.
 *
 * <p>{@code timestamp} and {@code ssrc} are unsigned 32-bit values held in a
 * {@code long}, so neither turns negative in arithmetic or in a log line.
 */
public record RtpPacket(
        int flags,
        boolean marker,
        int payloadType,
        int sequenceNumber,
        long timestamp,
        long ssrc,
        ByteBuf body) {

    /** Bytes in the fixed header. */
    public static final int HEADER_LENGTH = 12;

    /** Static payload type for MPEG-2 transport streams (RFC 3551 §6, RFC 2250). */
    public static final int PAYLOAD_TYPE_MP2T = 33;

    private static final int VERSION = 2;

    /**
     * A packet with no CSRCs, extension or padding, which is every packet Press
     * itself sends.
     */
    public static RtpPacket of(boolean marker, int payloadType, int sequenceNumber, long timestamp, long ssrc,
            ByteBuf payload) {
        return new RtpPacket(0, marker, payloadType, sequenceNumber, timestamp, ssrc, payload);
    }

    /** The P bit: the body ends in padding whose last byte is its own length. */
    public boolean padding() {
        return (flags & 0x20) != 0;
    }

    /** The X bit: a header extension follows the CSRC list. */
    public boolean extension() {
        return (flags & 0x10) != 0;
    }

    /** How many contributing-source identifiers start the body. */
    public int csrcCount() {
        return flags & 0x0F;
    }

    /**
     * The media payload: the body without its CSRC list, header extension and
     * padding. A slice sharing the body's memory and reference count, not a
     * retained one; it is valid for exactly as long as the body is.
     */
    public ByteBuf payload() {
        int start = body.readerIndex() + 4 * csrcCount();
        int end = body.writerIndex();
        if (extension()) {
            int extensionWords = body.getUnsignedShort(start + 2);
            start += 4 + 4 * extensionWords;
        }
        if (padding()) {
            end -= body.getUnsignedByte(end - 1);
        }
        return body.slice(start, end - start);
    }

    /**
     * Reads one packet from a datagram, or returns {@code null} when it fails
     * the validity checks of RFC 3550 Appendix A.1: version 2, a payload type
     * that is not SR or RR (which would make this an RTCP packet on the wrong
     * port), and CSRC, extension and padding lengths that fit the datagram.
     *
     * <p>The packet's body is a retained slice of {@code datagram}. The caller
     * still owns {@code datagram} and releases it as usual; the packet owns one
     * reference, released with {@code body().release()}.
     */
    public static RtpPacket decode(ByteBuf datagram) {
        int start = datagram.readerIndex();
        int length = datagram.readableBytes();
        if (length < HEADER_LENGTH) {
            return null;
        }
        int b0 = datagram.getUnsignedByte(start);
        int b1 = datagram.getUnsignedByte(start + 1);
        if (b0 >> 6 != VERSION) {
            return null;
        }
        int payloadType = b1 & 0x7F;
        // RTCP SR (200) and RR (201) read as PT 72 and 73 with the marker set.
        if (payloadType == 72 || payloadType == 73) {
            return null;
        }
        int flags = b0 & 0x3F;
        int bodyLength = length - HEADER_LENGTH;
        int consumed = 4 * (flags & 0x0F);
        if (consumed > bodyLength) {
            return null;
        }
        if ((flags & 0x10) != 0) {
            if (consumed + 4 > bodyLength) {
                return null;
            }
            consumed += 4 + 4 * datagram.getUnsignedShort(start + HEADER_LENGTH + consumed + 2);
            if (consumed > bodyLength) {
                return null;
            }
        }
        if ((flags & 0x20) != 0) {
            int paddingLength = bodyLength == 0 ? 0 : datagram.getUnsignedByte(start + length - 1);
            if (paddingLength == 0 || consumed + paddingLength > bodyLength) {
                return null;
            }
        }
        return new RtpPacket(
                flags,
                (b1 & 0x80) != 0,
                payloadType,
                datagram.getUnsignedShort(start + 2),
                datagram.getUnsignedInt(start + 4),
                datagram.getUnsignedInt(start + 8),
                datagram.retainedSlice(start + HEADER_LENGTH, bodyLength));
    }

    /** Writes the fixed header and the body to {@code out}. Does not release the body. */
    public void encodeTo(ByteBuf out) {
        out.writeByte(VERSION << 6 | flags & 0x3F);
        out.writeByte((marker ? 0x80 : 0) | payloadType & 0x7F);
        out.writeShort(sequenceNumber);
        out.writeInt((int) timestamp);
        out.writeInt((int) ssrc);
        out.writeBytes(body, body.readerIndex(), body.readableBytes());
    }

    /** The whole packet in a new buffer. Does not release the body. */
    public ByteBuf encode(ByteBufAllocator allocator) {
        ByteBuf out = allocator.buffer(HEADER_LENGTH + body.readableBytes());
        encodeTo(out);
        return out;
    }
}