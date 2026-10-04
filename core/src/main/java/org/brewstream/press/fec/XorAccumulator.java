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
import org.brewstream.press.packet.RtpPacket;

/**
 * The XOR of a set of RTP packets, field by field, as SMPTE 2022-1 (after
 * RFC 2733) defines it: P/X/CC bits, marker, payload type, timestamp, body
 * length, and the body itself, each body zero-padded to the longest. One
 * operation serves both directions. Encoding XORs a row or column together;
 * decoding XORs the FEC packet with every packet that did arrive, which leaves
 * the one that did not.
 */
final class XorAccumulator {

    private final ByteBufAllocator allocator;
    private int flags;
    private boolean marker;
    private int payloadType;
    private long timestamp;
    private int length;
    private ByteBuf body;

    XorAccumulator(ByteBufAllocator allocator) {
        this.allocator = allocator;
    }

    /** Starts from an FEC packet's recovery fields and payload. */
    void start(int flags, boolean marker, int payloadType, long timestamp, int length, ByteBuf payload) {
        this.flags = flags;
        this.marker = marker;
        this.payloadType = payloadType;
        this.timestamp = timestamp;
        this.length = length;
        this.body = allocator.buffer(payload.readableBytes());
        body.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
    }

    /** Folds one packet in. */
    void add(RtpPacket packet) {
        if (body == null) {
            body = allocator.buffer(packet.body().readableBytes());
        }
        flags ^= packet.flags();
        marker ^= packet.marker();
        payloadType ^= packet.payloadType();
        timestamp ^= packet.timestamp();
        ByteBuf in = packet.body();
        int inLength = in.readableBytes();
        length ^= inLength;
        if (inLength > body.writerIndex()) {
            body.writeZero(inLength - body.writerIndex());
        }
        int from = in.readerIndex();
        int i = 0;
        for (; i + 8 <= inLength; i += 8) {
            body.setLong(i, body.getLong(i) ^ in.getLong(from + i));
        }
        for (; i < inLength; i++) {
            body.setByte(i, body.getByte(i) ^ in.getByte(from + i));
        }
    }

    int flags() {
        return flags & 0x3F;
    }

    boolean marker() {
        return marker;
    }

    int payloadType() {
        return payloadType & 0x7F;
    }

    long timestamp() {
        return timestamp & 0xFFFF_FFFFL;
    }

    int length() {
        return length & 0xFFFF;
    }

    /**
     * The accumulated body. Ownership passes to the caller, and the accumulator
     * starts over: every field back to zero, ready for the next row or column.
     * Read the other fields first.
     */
    ByteBuf takeBody() {
        ByteBuf taken = body == null ? allocator.buffer(0) : body;
        body = null;
        flags = 0;
        marker = false;
        payloadType = 0;
        timestamp = 0;
        length = 0;
        return taken;
    }

    /** Releases the body if it was never taken. */
    void release() {
        if (body != null) {
            body.release();
            body = null;
        }
    }
}