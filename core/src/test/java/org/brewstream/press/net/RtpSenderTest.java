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

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RtpSenderTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    private RtpSender sender;
    private RtpReceiver receiver;
    private final ByteArrayOutputStream received = new ByteArrayOutputStream();

    @AfterEach
    void tearDown() throws InterruptedException {
        if (sender != null) {
            sender.close();
        }
        if (receiver != null) {
            receiver.close();
        }
    }

    @Test
    void packsWritesOfAnySizeIntoFullPacketsAndDeliversTheSameBytes() throws Exception {
        startReceiver(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        sender = RtpSender.connect(RtpSenderConfig.to(receiver.localAddress()));
        byte[] stream = tsBytes(70);

        // Uneven writes: none lines up with a packet or a TS packet.
        for (int at = 0; at < stream.length; ) {
            int size = Math.min(stream.length - at, 1000 + at % 777);
            sender.write(Unpooled.wrappedBuffer(stream, at, size));
            at += size;
        }

        awaitBytes(stream.length);
        assertThat(receivedBytes()).isEqualTo(stream);
        assertThat(sender.stats().packetsSent()).isEqualTo(10);
        assertThat(sender.stats().bytesSent()).isEqualTo(stream.length);
        assertThat(receiver.stats().packetsLost()).isZero();
    }

    @Test
    void sendsRfc2250PacketsOfSevenTsPacketsWithAdvancingSequenceAndTimestamp() throws Exception {
        try (DatagramSocket socket = new DatagramSocket(0, LOOPBACK)) {
            socket.setSoTimeout(5000);
            sender = RtpSender.connect(RtpSenderConfig.to((InetSocketAddress) socket.getLocalSocketAddress())
                    .withRtcp(false));
            sender.write(Unpooled.wrappedBuffer(tsBytes(14)));

            RtpPacket first = receive(socket);
            Thread.sleep(20);
            RtpPacket second = receive(socket);

            assertThat(first.payloadType()).isEqualTo(RtpPacket.PAYLOAD_TYPE_MP2T);
            assertThat(first.payload().readableBytes()).isEqualTo(1316);
            assertThat(second.sequenceNumber()).isEqualTo((first.sequenceNumber() + 1) & 0xFFFF);
            assertThat(first.ssrc()).isEqualTo(sender.ssrc()).isEqualTo(second.ssrc());
            // Both were sent at once, so the 90 kHz send times are within a few ticks.
            assertThat((second.timestamp() - first.timestamp()) & 0xFFFF_FFFFL).isLessThan(900);
        }
    }

    @Test
    void flushSendsAPartialPacket() throws Exception {
        startReceiver(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        sender = RtpSender.connect(RtpSenderConfig.to(receiver.localAddress()));
        byte[] stream = tsBytes(3);

        sender.write(Unpooled.wrappedBuffer(stream));
        Thread.sleep(100);
        assertThat(receivedBytes()).isEmpty();
        sender.flush();

        awaitBytes(stream.length);
        assertThat(receivedBytes()).isEqualTo(stream);
    }

    /**
     * The receiver echoes the sender's report in its own, and the sender works
     * out the round trip from it (RFC 3550 §6.4.1). Closing the receiver sends a
     * report at once, so the test does not wait out the five-second timer.
     */
    @Test
    void learnsTheRoundTripTimeFromReceiverReports() throws Exception {
        startReceiver(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)));
        sender = RtpSender.connect(RtpSenderConfig.to(receiver.localAddress()));
        List<Long> goodbyes = new CopyOnWriteArrayList<>();
        sender.addListener(new RtpSenderListener() {
            @Override
            public void onGoodbye(RtpSender s, InetSocketAddress from, long ssrc) {
                goodbyes.add(ssrc);
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (receiver.stats().senderReports() == 0 && System.nanoTime() < deadline) {
            sender.write(Unpooled.wrappedBuffer(tsBytes(7)));
            Thread.sleep(20);
        }
        assertThat(receiver.stats().senderReports()).as("the receiver heard a sender report").isPositive();

        receiver.close();
        receiver = null;
        Thread.sleep(300);

        SenderStats stats = sender.stats();
        assertThat(stats.receiverReports()).isPositive();
        assertThat(stats.rttMicros()).isBetween(0L, 100_000L);
        assertThat(stats.cumulativeLost()).isZero();
        assertThat(goodbyes).hasSize(1);
    }

    @Test
    void protectsTheStreamWithFecThatAReceiverRecoversFrom() throws Exception {
        startReceiver(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0))
                .withFec(true).withLatency(Duration.ofMillis(500)));
        try (LossyLink link = new LossyLink(receiver.localAddress().getPort(), i -> i % 10 == 4 && i < 180)) {
            sender = RtpSender.connect(RtpSenderConfig.to(new InetSocketAddress(LOOPBACK, link.port()))
                    .withFec(5, 5));
            byte[] stream = tsBytes(7 * 250);
            for (int at = 0; at < stream.length; at += 1316) {
                sender.write(Unpooled.wrappedBuffer(stream, at, 1316));
                if (at % (1316 * 10) == 0) {
                    Thread.sleep(2);
                }
            }

            awaitBytes(stream.length);
            assertThat(receivedBytes()).isEqualTo(stream);
            assertThat(link.dropped()).isEqualTo(18);
            assertThat(receiver.stats().packetsRecovered()).isEqualTo(18);
            assertThat(sender.stats().fecPacketsSent()).isPositive();
        }
    }

    @Test
    void refusesFecMatricesOutsideTheStandard() {
        RtpSenderConfig config = RtpSenderConfig.to(new InetSocketAddress(LOOPBACK, 5000));

        assertThatThrownBy(() -> config.withFec(30, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> config.withTsPacketsPerDatagram(8)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writesAfterCloseAreReleasedNotSent() throws Exception {
        sender = RtpSender.connect(RtpSenderConfig.to(new InetSocketAddress(LOOPBACK, 5000)).withRtcp(false));
        sender.close();
        var late = Unpooled.wrappedBuffer(tsBytes(1));

        sender.write(late);

        assertThat(late.refCnt()).isZero();
        assertThat(sender.stats().packetsSent()).isZero();
    }

    // -------------------------------------------------------------------------

    private void startReceiver(RtpReceiverConfig config) throws Exception {
        receiver = RtpReceiver.bind(config);
        receiver.onData(payload -> {
            synchronized (received) {
                received.writeBytes(ByteBufUtil.getBytes(payload));
            }
            payload.release();
        });
    }

    private byte[] receivedBytes() {
        synchronized (received) {
            return received.toByteArray();
        }
    }

    private void awaitBytes(int count) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (receivedBytes().length < count) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("received " + receivedBytes().length + " of " + count + " bytes; "
                        + (receiver == null ? "" : receiver.stats()));
            }
            Thread.sleep(10);
        }
    }

    /** {@code count} TS packets, each starting with the sync byte, every byte different from its neighbours. */
    static byte[] tsBytes(int count) {
        byte[] bytes = new byte[count * 188];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = i % 188 == 0 ? 0x47 : (byte) (i * 13 + i / 188);
        }
        return bytes;
    }

    private static RtpPacket receive(DatagramSocket socket) throws Exception {
        byte[] buffer = new byte[2048];
        DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
        socket.receive(datagram);
        return RtpPacket.decode(Unpooled.wrappedBuffer(Arrays.copyOf(buffer, datagram.getLength())));
    }
}