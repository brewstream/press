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

package org.brewstream.press.net;

import org.brewstream.press.fec.FecEncoder;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Objects;

/**
 * Where and how a {@link RtpSender} sends.
 *
 * <pre>{@code
 * RtpSenderConfig.to(new InetSocketAddress("10.0.0.9", 5000)).withFec(5, 5);
 * RtpSenderConfig.to(new InetSocketAddress("239.1.1.1", 5000)).withTtl(16).withInterface(eth0);
 * }</pre>
 *
 * <p>Media goes to the destination's port P, sender reports to P+1, and, with FEC,
 * column FEC to P+2 and row FEC to P+4, matching what {@link RtpReceiver} and
 * every common receiver expect. The sender sends from a local pair: media from
 * port Q and RTCP from Q+1, because receivers such as ffmpeg send their reports
 * to the media source's port plus one.
 *
 * @param destination        the receiver's base address and port P, unicast or multicast
 * @param localAddress       local address and base port Q, or port 0 for any free pair
 * @param payloadType        RTP payload type: 33 (MP2T, RFC 3551) unless the receiver was told
 *                           otherwise out of band
 * @param tsPacketsPerDatagram transport packets per RTP packet, 1 to 7. Seven makes 1316 bytes of
 *                           payload, the largest that fits a 1500-byte Ethernet MTU with headers
 * @param fecColumns         L, or 0 for no FEC
 * @param fecRows            D, or 0 for no FEC
 * @param rtcp               send sender reports, and listen for receiver reports
 * @param ttl                multicast time-to-live, or -1 to leave the system default
 * @param networkInterface   the interface to send multicast from, or {@code null} for the default
 * @param sendBufferBytes    the socket send buffer to ask the OS for
 */
public record RtpSenderConfig(
        InetSocketAddress destination,
        InetSocketAddress localAddress,
        int payloadType,
        int tsPacketsPerDatagram,
        int fecColumns,
        int fecRows,
        boolean rtcp,
        int ttl,
        NetworkInterface networkInterface,
        int sendBufferBytes) {

    public static final int DEFAULT_SEND_BUFFER_BYTES = 1024 * 1024;

    public RtpSenderConfig {
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(localAddress, "localAddress");
        if (destination.isUnresolved()) {
            throw new IllegalArgumentException("destination " + destination + " is unresolved");
        }
        if (payloadType < 0 || payloadType > 127 || payloadType == 72 || payloadType == 73) {
            throw new IllegalArgumentException("payload type " + payloadType + " is not a valid RTP payload type");
        }
        if (tsPacketsPerDatagram < 1 || tsPacketsPerDatagram > 7) {
            throw new IllegalArgumentException("tsPacketsPerDatagram must be 1 to 7, not " + tsPacketsPerDatagram);
        }
        if (fecColumns != 0 || fecRows != 0) {
            FecEncoder.validate(fecColumns, fecRows);
        }
        int highest = destination.getPort() + (fecColumns > 0 ? 4 : rtcp ? 1 : 0);
        if (highest > 65535) {
            throw new IllegalArgumentException("destination port " + destination.getPort()
                    + " leaves no room for the RTCP and FEC ports above it");
        }
        if (sendBufferBytes <= 0) {
            throw new IllegalArgumentException("sendBufferBytes must be positive");
        }
    }

    /** Sends to {@code destination} from any free local port pair, MP2T, seven TS packets per datagram, RTCP on. */
    public static RtpSenderConfig to(InetSocketAddress destination) {
        return new RtpSenderConfig(destination, new InetSocketAddress(0), 33, 7, 0, 0, true, -1, null,
                DEFAULT_SEND_BUFFER_BYTES);
    }

    public boolean fec() {
        return fecColumns > 0;
    }

    public RtpSenderConfig withLocalAddress(InetSocketAddress localAddress) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withPayloadType(int payloadType) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withTsPacketsPerDatagram(int tsPacketsPerDatagram) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    /** SMPTE 2022-1 FEC with an L-by-D matrix: L 1-20, D 4-20, L times D at most 100. */
    public RtpSenderConfig withFec(int columns, int rows) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, columns, rows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withoutFec() {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, 0, 0,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withRtcp(boolean rtcp) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withTtl(int ttl) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withInterface(NetworkInterface networkInterface) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }

    public RtpSenderConfig withSendBufferBytes(int sendBufferBytes) {
        return new RtpSenderConfig(destination, localAddress, payloadType, tsPacketsPerDatagram, fecColumns, fecRows,
                rtcp, ttl, networkInterface, sendBufferBytes);
    }
}