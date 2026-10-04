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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.press.packet.NtpTime;
import org.brewstream.press.packet.RtcpPacket;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RtpReceiverTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final long SSRC = 0x1234_5678L;

    private RtpReceiver receiver;
    private DatagramSocket sender;
    private final BlockingQueue<Integer> payloads = new LinkedBlockingQueue<>();
    private final List<String> events = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() throws InterruptedException {
        if (receiver != null) {
            receiver.close();
        }
        if (sender != null) {
            sender.close();
        }
    }

    @Test
    void deliversPayloadsInSequenceOrder() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));

        for (int seq : new int[]{0, 1, 3, 2, 4}) {
            send(SSRC, seq);
        }

        assertThat(take(5)).containsExactly(0, 1, 2, 3, 4);
        ReceiverStats stats = receiver.stats();
        assertThat(stats.ssrc()).isEqualTo(SSRC);
        assertThat(stats.packetsDelivered()).isEqualTo(5);
        assertThat(stats.packetsLost()).isZero();
        assertThat(stats.bytesDelivered()).isEqualTo(5 * 188);
    }

    @Test
    void givesUpOnAMissingPacketAfterTheLatencyAndSaysSo() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)).withLatency(Duration.ofMillis(50)));

        send(SSRC, 10);
        send(SSRC, 11);
        send(SSRC, 13);

        assertThat(take(3)).containsExactly(10, 11, 13);
        assertThat(events).contains("loss 12+1");
        ReceiverStats stats = receiver.stats();
        assertThat(stats.packetsLost()).isEqualTo(1);
        assertThat(stats.networkLost()).isEqualTo(1);
    }

    @Test
    void countsDuplicatesAndLatePacketsWithoutDeliveringThem() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)).withLatency(Duration.ofMillis(30)));

        send(SSRC, 1);
        send(SSRC, 3);
        assertThat(take(2)).containsExactly(1, 3);
        send(SSRC, 3);
        send(SSRC, 2);
        send(SSRC, 4);
        assertThat(take(1)).containsExactly(4);

        ReceiverStats stats = receiver.stats();
        assertThat(stats.packetsDuplicate()).isEqualTo(1);
        assertThat(stats.packetsLate()).isEqualTo(1);
        assertThat(payloads).isEmpty();
    }

    @Test
    void dropsAnotherSsrcWhileTheSourceIsActiveAndFollowsItOnceTheSourceFallsSilent() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        long other = 0x0BAD_F00DL;

        send(SSRC, 1);
        assertThat(take(1)).containsExactly(1);
        send(other, 500);
        Thread.sleep(100);
        assertThat(receiver.stats().packetsForeign()).isEqualTo(1);

        Thread.sleep(RtpReceiver.SOURCE_SWITCH_SILENCE_MILLIS);
        send(other, 501);
        send(other, 502);

        assertThat(take(2)).containsExactly(501 & 0xFF, 502 & 0xFF);
        assertThat(receiver.stats().ssrc()).isEqualTo(other);
        assertThat(events).contains("source " + SSRC + "->" + other);
    }

    /** RFC 3550 A.1: one far-off packet is dropped; a second in sequence with it is a restart. */
    @Test
    void resynchronisesWhenTheSameSsrcRestartsItsSequenceNumbers() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));

        send(SSRC, 100);
        send(SSRC, 101);
        assertThat(take(2)).containsExactly(100, 101);
        send(SSRC, 40_000);
        send(SSRC, 40_001);
        send(SSRC, 40_002);

        assertThat(take(2)).containsExactly(40_001 & 0xFF, 40_002 & 0xFF);
        assertThat(receiver.stats().sourceChanges()).isEqualTo(2);
        assertThat(events).contains("source " + SSRC + "->" + SSRC);
    }

    @Test
    void countsDatagramsThatAreNotRtp() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));

        sendRaw(new byte[]{1, 2, 3});
        sendRaw(new byte[20]); // version 0
        send(SSRC, 1);

        assertThat(take(1)).containsExactly(1);
        assertThat(receiver.stats().packetsInvalid()).isEqualTo(2);
    }

    /**
     * The receiver answers a sender report with a receiver report echoing it as
     * LSR, to the address the report came from. Close sends one at once along
     * with BYE, which is what this waits on rather than the five-second timer.
     */
    @Test
    void reportsBackToTheSenderAndSaysGoodbyeOnClose() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        try (DatagramSocket rtcpSocket = new DatagramSocket(0, LOOPBACK)) {
            rtcpSocket.setSoTimeout(5_000);
            send(SSRC, 1);
            send(SSRC, 3);
            long ntp = NtpTime.now();
            sendRtcp(rtcpSocket, new RtcpPacket.SenderReport(SSRC, ntp, 0, 3, 564, List.of()));
            assertThat(take(1)).containsExactly(1);
            awaitEvent("sr");

            receiver.close();

            List<RtcpPacket> compound = receiveRtcp(rtcpSocket);
            assertThat(compound).hasSize(3);
            RtcpPacket.ReceiverReport rr = (RtcpPacket.ReceiverReport) compound.get(0);
            RtcpPacket.ReportBlock block = rr.reports().getFirst();
            assertThat(block.ssrc()).isEqualTo(SSRC);
            assertThat(block.lastSenderReport()).isEqualTo(NtpTime.compact(ntp));
            assertThat(block.extendedHighestSeq()).isEqualTo(3);
            assertThat(block.cumulativeLost()).isEqualTo(1);
            assertThat(block.fractionLost()).isEqualTo(256 / 3);
            assertThat(compound.get(1)).isInstanceOf(RtcpPacket.SourceDescription.class);
            assertThat(compound.get(2)).isEqualTo(new RtcpPacket.Goodbye(List.of(rr.ssrc())));
        }
    }

    @Test
    void sendsReceiverReportsPeriodically() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        try (DatagramSocket rtcpSocket = new DatagramSocket(0, LOOPBACK)) {
            rtcpSocket.setSoTimeout(10_000);
            send(SSRC, 1);
            sendRtcp(rtcpSocket, new RtcpPacket.SenderReport(SSRC, NtpTime.now(), 0, 1, 188, List.of()));

            List<RtcpPacket> compound = receiveRtcp(rtcpSocket);

            assertThat(compound.getFirst()).isInstanceOf(RtcpPacket.ReceiverReport.class);
            assertThat(compound).hasSize(2);
        }
    }

    @Test
    void followsTheSourceSayingGoodbye() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        try (DatagramSocket rtcpSocket = new DatagramSocket(0, LOOPBACK)) {
            send(SSRC, 1);
            assertThat(take(1)).containsExactly(1);
            sendRtcp(rtcpSocket, new RtcpPacket.Goodbye(List.of(SSRC)));
            awaitEvent("bye " + SSRC);

            // No silence needed: the old source said it has stopped.
            send(77, 9);
            assertThat(take(1)).containsExactly(9);
        }
    }

    /** Port 0 with RTCP finds a base port P where P+1 is free too. */
    @Test
    void anEphemeralReceiverStillGetsRtcpOnThePortAbove() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        int base = receiver.localAddress().getPort();

        assertThat(canBind(base + 1)).as("RTCP port is held by the receiver").isFalse();
    }

    @Test
    void releasesItsPortsBeforeCloseReturns() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        int base = receiver.localAddress().getPort();

        receiver.close();
        receiver = null;

        assertThat(canBind(base)).isTrue();
        assertThat(canBind(base + 1)).isTrue();
    }

    /** close() from a listener runs on the receiver's own loop; it must not throw or leave sockets open. */
    @Test
    void closesFromItsOwnEventLoop() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        int base = receiver.localAddress().getPort();
        java.util.concurrent.CompletableFuture<Throwable> closedFromListener = new java.util.concurrent.CompletableFuture<>();
        receiver.addListener(new RtpReceiverListener() {
            @Override
            public void onSourceChanged(RtpReceiver r, long previous, long ssrc, InetSocketAddress source) {
                try {
                    r.close();
                    closedFromListener.complete(null);
                } catch (Throwable t) {
                    closedFromListener.complete(t);
                }
            }
        });

        send(SSRC, 1);

        assertThat(closedFromListener.get(5, TimeUnit.SECONDS)).isNull();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!(canBind(base) && canBind(base + 1)) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(canBind(base)).isTrue();
        assertThat(canBind(base + 1)).isTrue();
        receiver = null;
    }

    @Test
    void statsSurviveClose() throws Exception {
        start(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        send(SSRC, 1);
        assertThat(take(1)).containsExactly(1);

        receiver.close();

        assertThat(receiver.stats().packetsDelivered()).isEqualTo(1);
        receiver = null;
    }

    @Test
    void joinsAMulticastGroup() throws Exception {
        InetAddress group = InetAddress.getByName("239.255.42.42");
        NetworkInterface nic = interfaceWhereMulticastLoopsBack(group);
        Assumptions.assumeTrue(nic != null, "no interface delivers multicast back to this host");
        start(RtpReceiverConfig.multicast(group, 0).withInterface(nic));
        int port = receiver.localAddress().getPort();

        try (MulticastSocket multicast = new MulticastSocket(0)) {
            multicast.setOption(StandardSocketOptions.IP_MULTICAST_IF, nic);
            for (int seq = 1; seq <= 3; seq++) {
                ByteBuf wire = rtp(SSRC, seq);
                byte[] bytes = ByteBufUtil.getBytes(wire);
                wire.release();
                multicast.send(new DatagramPacket(bytes, bytes.length, group, port));
            }
        }

        assertThat(take(3)).containsExactly(1, 2, 3);
    }

    /**
     * An interface on which a plain JDK socket gets its own multicast back, so a
     * failure in the test above is Press's and not the host's. macOS does not
     * loop multicast back on lo0, so the loopback interface is tried last.
     */
    private static NetworkInterface interfaceWhereMulticastLoopsBack(InetAddress group) throws IOException {
        List<NetworkInterface> candidates = new ArrayList<>();
        for (NetworkInterface nic : java.util.Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (nic.isUp() && nic.supportsMulticast()
                    && nic.inetAddresses().anyMatch(a -> a instanceof java.net.Inet4Address)) {
                candidates.add(nic.isLoopback() ? candidates.size() : 0, nic);
            }
        }
        for (NetworkInterface nic : candidates) {
            try (MulticastSocket in = new MulticastSocket(0); MulticastSocket out = new MulticastSocket(0)) {
                in.joinGroup(new InetSocketAddress(group, 0), nic);
                in.setSoTimeout(500);
                out.setOption(StandardSocketOptions.IP_MULTICAST_IF, nic);
                out.send(new DatagramPacket(new byte[]{1}, 1, group, in.getLocalPort()));
                in.receive(new DatagramPacket(new byte[1], 1));
                return nic;
            } catch (IOException e) {
                // Try the next one.
            }
        }
        return null;
    }

    /**
     * A handler added in the bind initializer sees every packet: the media socket
     * reads nothing until the initializer has run. Packets already flowing when
     * the receiver binds queue in the socket and are delivered to it.
     */
    @Test
    void handlersAddedAtBindSeeEveryPacketDelivered() throws Exception {
        int port = freePortPair();
        java.util.concurrent.atomic.AtomicLong seen = new java.util.concurrent.atomic.AtomicLong();
        try (Blaster blaster = new Blaster(port)) {
            Thread.sleep(50);
            receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, port)),
                    pipeline -> pipeline.addLast(counter(seen)));
            Thread.sleep(200);
        }
        Thread.sleep(200);

        assertThat(receiver.stats().packetsDelivered()).isPositive();
        assertThat(seen.get()).isEqualTo(receiver.stats().packetsDelivered());
    }

    /**
     * Netty adds a handler from another thread asynchronously, and packets read
     * before it is in place pass it by. Linux CI hit that by chance before onData
     * waited for the add. The window is too narrow to hit on purpose, so this
     * tests the guarantee directly: with the event loop held busy the handler
     * cannot be added, so onData must not have returned; once the loop is free
     * it returns, and a packet sent afterwards reaches the handler.
     */
    @Test
    void onDataReturnsOnlyOnceItsHandlerIsInPlace() throws Exception {
        // A bare receiver: start()'s own onData handler would consume everything first.
        receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)).withRtcp(false));
        sender = new DatagramSocket(0, LOOPBACK);
        BlockingQueue<Integer> seen = new LinkedBlockingQueue<>();
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        receiver.channel().eventLoop().execute(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            java.util.concurrent.CompletableFuture<Void> added =
                    java.util.concurrent.CompletableFuture.runAsync(() -> {
                        try {
                            receiver.onData(payload -> {
                                seen.add((int) payload.getUnsignedByte(payload.readerIndex() + 1));
                                payload.release();
                            });
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        }
                    });
            Thread.sleep(200);
            assertThat(added).as("onData returned while its handler could not have been added").isNotDone();

            release.countDown();
            added.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown(); // never leave the loop parked, or close() would wait forever
        }
        send(SSRC, 7);
        assertThat(seen.poll(5, TimeUnit.SECONDS)).isEqualTo(7);
    }

    private static void sendTo(DatagramSocket socket, int port, ByteBuf wire) throws IOException {
        byte[] bytes = ByteBufUtil.getBytes(wire);
        wire.release();
        socket.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, port));
    }

    private static io.netty.channel.ChannelHandler counter(java.util.concurrent.atomic.AtomicLong seen) {
        return new io.netty.channel.SimpleChannelInboundHandler<ByteBuf>() {
            @Override
            protected void channelRead0(io.netty.channel.ChannelHandlerContext ctx, ByteBuf payload) {
                seen.incrementAndGet();
            }
        };
    }

    /** Sends in-sequence RTP packets to a port as fast as it can until closed. */
    private static final class Blaster implements AutoCloseable {
        private final Thread thread;
        private volatile boolean running = true;

        Blaster(int port) {
            thread = new Thread(() -> {
                try (DatagramSocket socket = new DatagramSocket(0, LOOPBACK)) {
                    for (int seq = 0; running; seq++) {
                        ByteBuf wire = rtp(SSRC, seq);
                        byte[] bytes = ByteBufUtil.getBytes(wire);
                        wire.release();
                        socket.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, port));
                        if (seq % 16 == 0) {
                            Thread.sleep(1);
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    // stop
                }
            }, "rtp-blaster");
            thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            thread.join();
        }
    }

    /** A free even port P with P+1 free too, released for the receiver to take. */
    private static int freePortPair() throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            int port;
            try (DatagramSocket probe = new DatagramSocket(0, LOOPBACK)) {
                port = probe.getLocalPort() & ~1;
            }
            if (port > 0 && canBind(port) && canBind(port + 1)) {
                return port;
            }
        }
        throw new IOException("no free port pair");
    }

    // --- helpers -------------------------------------------------------------

    private void start(RtpReceiverConfig config) throws Exception {
        receiver = RtpReceiver.bind(config);
        receiver.onData(payload -> {
            payloads.add((int) payload.getUnsignedByte(payload.readerIndex() + 1));
            payload.release();
        });
        receiver.addListener(new RtpReceiverListener() {
            @Override
            public void onLoss(RtpReceiver r, long first, int count) {
                events.add("loss " + first + "+" + count);
            }

            @Override
            public void onSourceChanged(RtpReceiver r, long previous, long ssrc, InetSocketAddress source) {
                events.add("source " + previous + "->" + ssrc);
            }

            @Override
            public void onSenderReport(RtpReceiver r, RtcpPacket.SenderReport report) {
                events.add("sr");
            }

            @Override
            public void onGoodbye(RtpReceiver r, long ssrc) {
                events.add("bye " + ssrc);
            }
        });
        sender = new DatagramSocket(0, LOOPBACK);
    }

    /** One RTP packet carrying a single TS packet whose second byte is the low byte of {@code seq}. */
    private static ByteBuf rtp(long ssrc, int seq) {
        ByteBuf payload = Unpooled.buffer(188);
        payload.writeByte(0x47);
        payload.writeByte(seq & 0xFF);
        payload.writeZero(186);
        ByteBuf wire = RtpPacket.of(false, 33, seq & 0xFFFF, seq * 90L, ssrc, payload)
                .encode(ByteBufAllocator.DEFAULT);
        payload.release();
        return wire;
    }

    private void send(long ssrc, int seq) throws IOException {
        ByteBuf wire = rtp(ssrc, seq);
        sendRaw(ByteBufUtil.getBytes(wire));
        wire.release();
    }

    private void sendRaw(byte[] bytes) throws IOException {
        sender.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, receiver.localAddress().getPort()));
    }

    private void sendRtcp(DatagramSocket from, RtcpPacket packet) throws IOException {
        ByteBuf out = Unpooled.buffer();
        RtcpPacket.encode(packet, out);
        byte[] bytes = ByteBufUtil.getBytes(out);
        from.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, receiver.localAddress().getPort() + 1));
    }

    private static List<RtcpPacket> receiveRtcp(DatagramSocket socket) throws IOException {
        byte[] buffer = new byte[1500];
        DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
        try {
            socket.receive(datagram);
        } catch (SocketTimeoutException e) {
            throw new AssertionError("no RTCP from the receiver", e);
        }
        return RtcpPacket.decodeCompound(Unpooled.wrappedBuffer(Arrays.copyOf(buffer, datagram.getLength())));
    }

    private List<Integer> take(int count) throws InterruptedException {
        List<Integer> taken = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Integer next = payloads.poll(5, TimeUnit.SECONDS);
            if (next == null) {
                throw new AssertionError("only " + taken + " delivered, wanted " + count + "; events " + events);
            }
            taken.add(next);
        }
        return taken;
    }

    private void awaitEvent(String event) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!events.contains(event)) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no '" + event + "' in " + events);
            }
            Thread.sleep(10);
        }
    }

    private static boolean canBind(int port) {
        try (DatagramSocket socket = new DatagramSocket(port, LOOPBACK)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}