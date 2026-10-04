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
import org.brewstream.press.fec.FecEncoder;
import org.brewstream.press.packet.FecPacket;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** FEC through a real receiver: media to P, column FEC to P+2, row FEC to P+4, some media dropped. */
class RtpReceiverFecTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final int L = 5;
    private static final int D = 5;

    private RtpReceiver receiver;
    private final BlockingQueue<Integer> delivered = new LinkedBlockingQueue<>();
    private final List<Long> recoveredEvents = new java.util.concurrent.CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() throws InterruptedException {
        if (receiver != null) {
            receiver.close();
        }
    }

    @Test
    void deliversTheWholeStreamWhenFecCanCoverTheLosses() throws Exception {
        start(Duration.ofMillis(500));
        // One loss in each of several rows, and a burst of L in the third matrix.
        Set<Integer> lost = Set.of(3, 17, 31, 52, 53, 54, 55, 56);

        send(0, 4 * L * D, lost, 0);

        List<Integer> got = take(3 * L * D);
        for (int i = 0; i < got.size(); i++) {
            assertThat(got.get(i)).as("packet " + i).isEqualTo(i & 0xFF);
        }
        ReceiverStats stats = receiver.stats();
        // Loopback can drop a packet of its own under load; FEC recovers that one too.
        // What must hold is that every packet the network lost was recovered, and
        // that a recovery the original overtook was not counted.
        assertThat(stats.networkLost()).as("the network still lost them").isGreaterThanOrEqualTo(lost.size());
        assertThat(stats.packetsRecovered()).as(stats.toString()).isEqualTo(stats.networkLost());
        assertThat(stats.packetsLost()).isZero();
        assertThat(stats.packetsRecoveredLate()).isZero();
        assertThat(stats.fecColumns()).isEqualTo(L);
        assertThat(stats.fecRows()).isEqualTo(D);
        // Events fire on recovery; FEC that overtook its media can add some for packets never lost.
        assertThat(recoveredEvents).containsAll(lost.stream().map(Integer::longValue).toList());
    }

    /**
     * Column FEC arrives a matrix after its packets. With a latency shorter than
     * that, delivery gives up on the gap first, and the recovery is counted as
     * late rather than delivered out of order.
     */
    @Test
    void countsARecoveryThatComesTooLate() throws Exception {
        start(Duration.ofMillis(20));
        // A burst only the columns can repair; their FEC is sent through the next matrix.
        Set<Integer> lost = Set.of(5, 6, 7);

        send(0, 2 * L * D + 1, lost, 3);

        Thread.sleep(500);
        ReceiverStats stats = receiver.stats();
        assertThat(stats.packetsLost()).isEqualTo(3);
        assertThat(stats.packetsRecovered()).isEqualTo(3);
        assertThat(stats.packetsRecoveredLate()).isEqualTo(3);
    }

    /**
     * FEC carries SSRC 0, so only the sender's address ties it to the stream. A
     * datagram on the FEC port from another address must not repair anything.
     */
    @Test
    void ignoresFecFromAnotherHost() throws Exception {
        InetAddress other = java.net.NetworkInterface.networkInterfaces()
                .flatMap(java.net.NetworkInterface::inetAddresses)
                .filter(a -> a instanceof java.net.Inet4Address && !a.isLoopbackAddress())
                .findFirst().orElse(null);
        org.junit.jupiter.api.Assumptions.assumeTrue(other != null, "no second local address to send from");
        start(Duration.ofMillis(200));
        int base = receiver.localAddress().getPort();
        try (DatagramSocket source = new DatagramSocket(0, LOOPBACK);
                DatagramSocket foreign = new DatagramSocket(0, other)) {
            sendMedia(source, base, 100);
            awaitReceived(1);

            // A row protecting 100 and 101: would rebuild 101 if it were accepted.
            sendFec(foreign, base + 4, new FecPacket(1, 0, 0, false, 100, 0, 0, 0, true, 1, 2,
                    Unpooled.buffer(1316).writeZero(1316)));
            Thread.sleep(300);

            assertThat(receiver.stats().packetsRecovered()).isZero();
            assertThat(receiver.stats().packetsForeign()).isEqualTo(1);
        }
    }

    /**
     * A malformed rebuilt packet used to throw on the receiver's timer, which
     * Netty then never ran again, so gaps stopped timing out. Here the FEC
     * claims a header extension a one-byte body cannot hold. The gap it tried to
     * fill, and a later one, must still time out.
     */
    @Test
    void malformedFecLeavesGapsTimingOut() throws Exception {
        start(Duration.ofMillis(100));
        int base = receiver.localAddress().getPort();
        try (DatagramSocket source = new DatagramSocket(0, LOOPBACK)) {
            sendMedia(source, base, 0);
            sendMedia(source, base, 3);
            awaitReceived(2);
            sendFec(source, base + 4, new FecPacket(1, 0, 0x10, false, 2, 1316 ^ 1, 0, 0, true, 1, 2,
                    Unpooled.buffer(1316).writeZero(1316)));

            assertThat(take(2)).containsExactly(0, 3);
            sendMedia(source, base, 6);
            assertThat(take(1)).as("a later gap still times out").containsExactly(6);
        }
    }

    private static void sendMedia(DatagramSocket socket, int port, int seq) throws Exception {
        ByteBuf payload = Unpooled.buffer(1316);
        payload.writeByte(0x47);
        payload.writeByte(seq & 0xFF);
        payload.writeZero(1314);
        ByteBuf wire = RtpPacket.of(false, 33, seq, seq * 900L, 0x5EC0L, payload).encode(ByteBufAllocator.DEFAULT);
        payload.release();
        byte[] bytes = ByteBufUtil.getBytes(wire);
        wire.release();
        socket.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, port));
    }

    private static void sendFec(DatagramSocket socket, int port, FecPacket fec) throws Exception {
        ByteBuf wire = fec.encode(ByteBufAllocator.DEFAULT);
        fec.payload().release();
        byte[] bytes = ByteBufUtil.getBytes(wire);
        wire.release();
        socket.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, port));
    }

    private void awaitReceived(long count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (receiver.stats().packetsReceived() < count && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(receiver.stats().packetsReceived()).isEqualTo(count);
    }

    private void start(Duration latency) throws Exception {
        receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0))
                .withFec(true).withLatency(latency));
        receiver.onData(payload -> {
            delivered.add((int) payload.getUnsignedByte(payload.readerIndex() + 1));
            payload.release();
        });
        receiver.addListener(new RtpReceiverListener() {
            @Override
            public void onRecovered(RtpReceiver r, long extendedSeq) {
                recoveredEvents.add(extendedSeq);
            }
        });
    }

    /**
     * Sends media and FEC in the order a sender emits them, dropping the media
     * packets in {@code lost}, pausing {@code pauseMillis} between packets.
     */
    private void send(int first, int count, Set<Integer> lost, int pauseMillis) throws Exception {
        int base = receiver.localAddress().getPort();
        try (DatagramSocket socket = new DatagramSocket(0, LOOPBACK)) {
            List<Object[]> wire = new ArrayList<>();
            FecEncoder encoder = new FecEncoder(L, D, new FecEncoder.Sink() {
                @Override
                public void column(FecPacket packet) {
                    wire.add(new Object[]{packet.encode(ByteBufAllocator.DEFAULT), base + 2});
                    packet.payload().release();
                }

                @Override
                public void row(FecPacket packet) {
                    wire.add(new Object[]{packet.encode(ByteBufAllocator.DEFAULT), base + 4});
                    packet.payload().release();
                }
            }, ByteBufAllocator.DEFAULT);
            for (int seq = first; seq < first + count; seq++) {
                ByteBuf payload = Unpooled.buffer(1316);
                payload.writeByte(0x47);
                payload.writeByte(seq & 0xFF);
                payload.writeZero(1314);
                RtpPacket packet = RtpPacket.of(false, 33, seq & 0xFFFF, seq * 900L, 0x5EC0L, payload);
                if (!lost.contains(seq)) {
                    wire.add(new Object[]{packet.encode(ByteBufAllocator.DEFAULT), base});
                }
                encoder.onMedia(packet);
                payload.release();
                for (Object[] item : wire) {
                    ByteBuf out = (ByteBuf) item[0];
                    byte[] bytes = ByteBufUtil.getBytes(out);
                    out.release();
                    socket.send(new DatagramPacket(bytes, bytes.length, LOOPBACK, (int) item[1]));
                }
                wire.clear();
                if (pauseMillis > 0) {
                    Thread.sleep(pauseMillis);
                } else if (seq % 10 == 9) {
                    Thread.sleep(1); // a burst, but not one a loaded machine's loopback drops from
                }
            }
            encoder.close();
        }
    }

    private List<Integer> take(int count) throws InterruptedException {
        List<Integer> taken = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Integer next = delivered.poll(5, TimeUnit.SECONDS);
            if (next == null) {
                throw new AssertionError("only " + taken.size() + " delivered, wanted " + count
                        + "; stats " + receiver.stats());
            }
            taken.add(next);
        }
        return taken;
    }
}