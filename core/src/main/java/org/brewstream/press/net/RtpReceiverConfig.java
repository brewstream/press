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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.time.Duration;
import java.util.Objects;

/**
 * Where and how a {@link RtpReceiver} listens.
 *
 * <p>The ports follow the usual convention: media on the base port P and RTCP
 * on P+1. With base port 0 the receiver picks a free base for which every port
 * it needs is free, so an ephemeral receiver can still have RTCP.
 *
 * <pre>{@code
 * RtpReceiverConfig.unicast(5000).withLatency(Duration.ofMillis(300));
 * RtpReceiverConfig.multicast(InetAddress.getByName("239.1.1.1"), 5000).withInterface(eth0);
 * }</pre>
 *
 * @param bindAddress      local address and base port. For multicast, the port, with a
 *                         wildcard address
 * @param multicastGroup   the group to join, or {@code null} for unicast
 * @param networkInterface the interface to join the group on, or {@code null} for the
 *                         system's choice. Ignored for unicast
 * @param sourceFilter     source-specific multicast (SSM): accept the group only from
 *                         this sender. {@code null} for any-source
 * @param latency          how long a gap in the sequence is waited on before the packets
 *                         after it are delivered anyway. This bounds how much reordering is
 *                         repaired, and is the delay every packet behind a loss pays
 * @param rtcp             listen on P+1 and send receiver reports to the sender
 * @param receiveBufferBytes the socket receive buffer to ask the OS for. Contribution
 *                         streams arrive in bursts that overrun small default buffers
 */
public record RtpReceiverConfig(
        InetSocketAddress bindAddress,
        InetAddress multicastGroup,
        NetworkInterface networkInterface,
        InetAddress sourceFilter,
        Duration latency,
        boolean rtcp,
        int receiveBufferBytes) {

    /** A short wait that absorbs reordering on a clean network. */
    public static final Duration DEFAULT_LATENCY = Duration.ofMillis(120);

    /** 4 MiB: about 8 seconds of a 4 Mbps stream, or a burst of 3000 full packets. */
    public static final int DEFAULT_RECEIVE_BUFFER_BYTES = 4 * 1024 * 1024;

    public RtpReceiverConfig {
        Objects.requireNonNull(bindAddress, "bindAddress");
        Objects.requireNonNull(latency, "latency");
        if (latency.isNegative()) {
            throw new IllegalArgumentException("latency must not be negative: " + latency);
        }
        if (multicastGroup != null && !multicastGroup.isMulticastAddress()) {
            throw new IllegalArgumentException(multicastGroup + " is not a multicast address");
        }
        if (sourceFilter != null && multicastGroup == null) {
            throw new IllegalArgumentException("a source filter only applies to multicast");
        }
        if (rtcp && bindAddress.getPort() == 65535) {
            throw new IllegalArgumentException("base port 65535 leaves no room for RTCP on the port above it");
        }
        if (receiveBufferBytes <= 0) {
            throw new IllegalArgumentException("receiveBufferBytes must be positive");
        }
    }

    /** Unicast on every local address. Port 0 picks a free base port. */
    public static RtpReceiverConfig unicast(int port) {
        return unicast(new InetSocketAddress(port));
    }

    /** Unicast on one local address and base port. */
    public static RtpReceiverConfig unicast(InetSocketAddress bindAddress) {
        return new RtpReceiverConfig(bindAddress, null, null, null, DEFAULT_LATENCY, true,
                DEFAULT_RECEIVE_BUFFER_BYTES);
    }

    /** Any-source multicast on a group and base port. */
    public static RtpReceiverConfig multicast(InetAddress group, int port) {
        return new RtpReceiverConfig(new InetSocketAddress(port), group, null, null, DEFAULT_LATENCY, true,
                DEFAULT_RECEIVE_BUFFER_BYTES);
    }

    public RtpReceiverConfig withInterface(NetworkInterface networkInterface) {
        return new RtpReceiverConfig(bindAddress, multicastGroup, networkInterface, sourceFilter, latency, rtcp,
                receiveBufferBytes);
    }

    public RtpReceiverConfig withSource(InetAddress sourceFilter) {
        return new RtpReceiverConfig(bindAddress, multicastGroup, networkInterface, sourceFilter, latency, rtcp,
                receiveBufferBytes);
    }

    public RtpReceiverConfig withLatency(Duration latency) {
        return new RtpReceiverConfig(bindAddress, multicastGroup, networkInterface, sourceFilter, latency, rtcp,
                receiveBufferBytes);
    }

    public RtpReceiverConfig withRtcp(boolean rtcp) {
        return new RtpReceiverConfig(bindAddress, multicastGroup, networkInterface, sourceFilter, latency, rtcp,
                receiveBufferBytes);
    }

    public RtpReceiverConfig withReceiveBufferBytes(int receiveBufferBytes) {
        return new RtpReceiverConfig(bindAddress, multicastGroup, networkInterface, sourceFilter, latency, rtcp,
                receiveBufferBytes);
    }
}