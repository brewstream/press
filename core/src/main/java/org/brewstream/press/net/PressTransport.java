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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Objects;

/**
 * The Netty resources a receiver or sender runs on.
 *
 * <p>By default each receiver and sender gets its own NIO group and shuts it
 * down on close, which is right for a handful of streams. An application with
 * many, or one that already runs Netty, lends its own group with {@link
 * #shared}; Press then never shuts it down. Roast's {@code SrtTransport} makes
 * the same split, so an SRT and an RTP leg can share one group.
 *
 * <pre>{@code
 * EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
 * PressTransport transport = PressTransport.shared(group, NioDatagramChannel.class);
 * }</pre>
 */
public final class PressTransport {

    private final EventLoopGroup eventLoopGroup;
    private final Class<? extends DatagramChannel> channelType;
    private final boolean shutdownWithOwner;

    private PressTransport(EventLoopGroup eventLoopGroup, Class<? extends DatagramChannel> channelType,
            boolean shutdownWithOwner) {
        this.eventLoopGroup = eventLoopGroup;
        this.channelType = channelType;
        this.shutdownWithOwner = shutdownWithOwner;
    }

    /**
     * Runs on an application's existing Netty resources, which Press will never
     * shut down.
     *
     * @param eventLoopGroup the group to register channels on
     * @param channelType    the datagram channel type matching the group's transport,
     *                       such as {@code NioDatagramChannel} for an NIO group
     */
    public static PressTransport shared(EventLoopGroup eventLoopGroup,
            Class<? extends DatagramChannel> channelType) {
        Objects.requireNonNull(eventLoopGroup, "eventLoopGroup");
        Objects.requireNonNull(channelType, "channelType");
        return new PressTransport(eventLoopGroup, channelType, false);
    }

    /** A fresh single-thread NIO group, owned by whatever it is handed to. */
    static PressTransport owned() {
        return new PressTransport(
                new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()), NioDatagramChannel.class, true);
    }

    EventLoopGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    boolean shutdownWithOwner() {
        return shutdownWithOwner;
    }

    /**
     * A bootstrap on one event loop. Every channel belonging to one receiver or
     * sender is created on the same loop, which is what lets their shared state
     * go unsynchronised.
     *
     * <p>An NIO channel that is to join a multicast group must be created for the
     * group's address family, which the reflective {@code channel(Class)} form
     * cannot do. Native transports need no such hint.
     */
    Bootstrap bootstrap(EventLoop loop, InetAddress familyOf) {
        Bootstrap bootstrap = new Bootstrap().group(loop);
        if (channelType == NioDatagramChannel.class && familyOf != null) {
            SocketProtocolFamily family = familyOf instanceof Inet6Address
                    ? SocketProtocolFamily.INET6
                    : SocketProtocolFamily.INET;
            return bootstrap.channelFactory(() -> new NioDatagramChannel(family));
        }
        return bootstrap.channel(channelType);
    }
}