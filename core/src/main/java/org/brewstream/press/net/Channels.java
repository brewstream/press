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

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Promise;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

/** Channel housekeeping shared by receivers and senders. */
final class Channels {

    private Channels() {
    }

    /**
     * Closes {@code channel} and returns once its port can be bound again.
     *
     * <p>Netty's close future completes before an NIO channel gives up its port:
     * the JDK closes a channel registered with a selector during that selector's
     * next selection. The event loop selects before every batch of tasks and
     * only takes up scheduled tasks at the start of a batch, so a task scheduled
     * from inside the loop runs after that selection. It must be scheduled from
     * inside: a zero-delay task scheduled from another thread is already due,
     * and Netty runs it in the current batch. The same fix went into Roast 0.1.3,
     * where the reasoning was measured.
     */
    static void closeAndAwaitRelease(Channel channel) throws InterruptedException {
        if (channel == null) {
            return;
        }
        channel.close().sync();
        EventLoop loop = channel.eventLoop();
        if (loop.isShuttingDown()) {
            return;
        }
        Promise<Void> released = loop.newPromise();
        loop.execute(() -> loop.schedule(() -> released.setSuccess(null), 0, TimeUnit.NANOSECONDS));
        released.sync();
    }

    /**
     * An interface to join a multicast group on when none was configured: the
     * first one that is up, supports multicast and has an address of the group's
     * family, preferring a non-loopback one. Netty's own default looks the
     * interface up from the bound address, which for a wildcard bind is none.
     */
    static NetworkInterface defaultMulticastInterface(InetAddress group) throws SocketException {
        NetworkInterface loopback = null;
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface candidate = interfaces.nextElement();
            if (!candidate.isUp() || !candidate.supportsMulticast() || !hasAddressLike(candidate, group)) {
                continue;
            }
            if (!candidate.isLoopback()) {
                return candidate;
            }
            loopback = candidate;
        }
        if (loopback == null) {
            throw new SocketException("no multicast-capable interface for " + group.getHostAddress());
        }
        return loopback;
    }

    private static boolean hasAddressLike(NetworkInterface candidate, InetAddress group) {
        Enumeration<InetAddress> addresses = candidate.getInetAddresses();
        while (addresses.hasMoreElements()) {
            if (addresses.nextElement().getClass() == group.getClass()) {
                return true;
            }
        }
        return false;
    }
}