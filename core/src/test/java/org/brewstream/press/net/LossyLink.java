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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

/**
 * A UDP link between a sender and a receiver's base port P that drops chosen
 * media packets. Forwards P, P+1, P+2 and P+4 to the same offsets on the target.
 * Only the media port is lossy; RTCP and FEC always pass.
 */
final class LossyLink implements AutoCloseable {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    private final int base;
    private final List<DatagramSocket> sockets = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicInteger dropped = new AtomicInteger();

    /**
     * @param target base port of the receiver
     * @param drops  given the media packet's index (0 for the first), whether to drop it
     */
    LossyLink(int target, IntPredicate drops) throws IOException {
        this.base = freeRun();
        for (int offset : new int[]{0, 1, 2, 4}) {
            DatagramSocket socket = new DatagramSocket(base + offset, LOOPBACK);
            socket.setReceiveBufferSize(8 << 20);
            sockets.add(socket);
            boolean lossy = offset == 0;
            int to = target + offset;
            AtomicInteger index = new AtomicInteger(); // per port: only media indices matter
            Thread thread = new Thread(() -> forward(socket, to, lossy ? drops : i -> false, index),
                    "lossy-link-" + offset);
            threads.add(thread);
            thread.start();
        }
    }

    /** The base port senders should send to. */
    int port() {
        return base;
    }

    int dropped() {
        return dropped.get();
    }

    private void forward(DatagramSocket in, int to, IntPredicate drops, AtomicInteger index) {
        byte[] buffer = new byte[2048];
        try (DatagramSocket out = new DatagramSocket(0, LOOPBACK)) {
            while (!in.isClosed()) {
                DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
                in.receive(datagram);
                if (drops.test(index.getAndIncrement())) {
                    dropped.incrementAndGet();
                    continue;
                }
                out.send(new DatagramPacket(buffer, datagram.getLength(), LOOPBACK, to));
            }
        } catch (IOException e) {
            // closed
        }
    }

    @Override
    public void close() throws InterruptedException {
        sockets.forEach(DatagramSocket::close);
        for (Thread thread : threads) {
            thread.join();
        }
    }

    private static int freeRun() throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            int port;
            try (DatagramSocket probe = new DatagramSocket(0, LOOPBACK)) {
                port = probe.getLocalPort();
            }
            boolean free = port < 65530;
            for (int offset = 0; free && offset <= 4; offset++) {
                try (DatagramSocket socket = new DatagramSocket(port + offset, LOOPBACK)) {
                    // free
                } catch (SocketException e) {
                    free = false;
                }
            }
            if (free) {
                return port;
            }
        }
        throw new IOException("no run of five free ports");
    }
}