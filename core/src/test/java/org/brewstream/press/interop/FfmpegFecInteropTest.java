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

package org.brewstream.press.interop;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.grind.TsStreamStats;
import org.brewstream.press.fec.FecEncoder;
import org.brewstream.press.net.ReceiverStats;
import org.brewstream.press.net.RtpReceiver;
import org.brewstream.press.net.RtpReceiverConfig;
import org.brewstream.press.packet.FecPacket;
import org.brewstream.press.packet.RtpPacket;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SMPTE 2022-1 FEC against ffmpeg's {@code prompeg} protocol (Pro-MPEG CoP #3,
 * the layout 2022-1 adopted). ffmpeg only sends FEC, so the two directions are
 * checked differently: Press's decoder recovers what a lossy link drops from
 * ffmpeg's FEC, and Press's encoder, given the media ffmpeg sent, produces
 * FEC packets identical to ffmpeg's.
 */
@Tag("interop")
class FfmpegFecInteropTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final int L = 5;
    private static final int D = 5;
    /** Drops stop here, so the stream's unprotected tail (its last FEC is never sent) loses nothing. */
    private static final int DROP_BEFORE = 1200;

    /**
     * One packet in 23 dropped (rows repair those), plus a burst of L every 150
     * (columns repair those): 92 of the first 1200 packets, 7.7% loss, and every
     * one comes back.
     */
    private static final IntPredicate DROPS = index -> index < DROP_BEFORE
            && (index % 23 == 11 || index % 150 >= 70 && index % 150 < 70 + L);

    @Test
    void recoversEverythingAPacketLosingLinkDropsFromFfmpegsFec() throws Exception {
        Result result = receiveThroughLossyLink(true);

        assertThat(result.dropped).as("the link really dropped packets").isGreaterThan(50);
        assertThat(result.stats.packetsLost()).isZero();
        assertThat(result.stats.packetsRecovered()).isEqualTo(result.dropped);
        assertThat(result.stats.packetsRecoveredLate()).isZero();
        assertThat(result.stats.fecColumns()).isEqualTo(L);
        assertThat(result.stats.fecRows()).isEqualTo(D);
        assertThat(result.health.continuityErrors()).isZero();
        assertThat(result.frames).isEqualTo(Ffmpeg.FIXTURE_FRAMES);
    }

    /** The control: the same link without FEC damages the stream, so the test above is not vacuous. */
    @Test
    void withoutFecTheSameLinkDamagesTheStream() throws Exception {
        Result result = receiveThroughLossyLink(false);

        assertThat(result.stats.packetsLost()).isEqualTo(result.dropped);
        assertThat(result.health.continuityErrors()).isPositive();
    }

    /**
     * ffmpeg's FEC packets, compared with what Press's encoder makes from the
     * same media: every recovery field and every payload byte. Only the FEC
     * streams' own sequence numbers differ, because both start them at random.
     */
    @Test
    void encodesFecIdenticallyToFfmpeg() throws Exception {
        String ffmpeg = Ffmpeg.binary();
        Path fixture = Ffmpeg.fixture();
        int base = freePortRun();
        Map<Integer, List<byte[]>> captured = new HashMap<>();
        List<Capture> captures = new ArrayList<>();
        for (int offset : new int[]{0, 1, 2, 4}) {
            List<byte[]> into = new CopyOnWriteArrayList<>();
            captured.put(offset, into);
            captures.add(new Capture(base + offset, into));
        }
        try {
            try (Ffmpeg.Running sender = Ffmpeg.start(List.of(ffmpeg, "-v", "warning", "-re",
                    "-i", fixture.toString(), "-c", "copy", "-f", "rtp_mpegts",
                    "-fec", "prompeg=l=" + L + ":d=" + D, "rtp://127.0.0.1:" + base))) {
                assertThat(sender.waitFor(Ffmpeg.FIXTURE_SECONDS + 20)).isTrue();
            }
            Thread.sleep(200);
        } finally {
            for (Capture capture : captures) {
                capture.close();
            }
        }

        List<RtpPacket> media = new ArrayList<>();
        for (byte[] datagram : captured.get(0)) {
            media.add(RtpPacket.decode(Unpooled.wrappedBuffer(datagram)));
        }
        media.sort(Comparator.comparingInt(RtpPacket::sequenceNumber));
        for (int i = 1; i < media.size(); i++) {
            assertThat(media.get(i).sequenceNumber()).as("capture is gapless")
                    .isEqualTo((media.get(i - 1).sequenceNumber() + 1) & 0xFFFF);
        }

        Map<String, FecPacket> ours = new HashMap<>();
        FecEncoder encoder = new FecEncoder(L, D, new FecEncoder.Sink() {
            @Override
            public void column(FecPacket packet) {
                ours.put(key(packet), packet);
            }

            @Override
            public void row(FecPacket packet) {
                ours.put(key(packet), packet);
            }
        }, ByteBufAllocator.DEFAULT);
        media.forEach(encoder::onMedia);

        int compared = 0;
        for (int offset : new int[]{2, 4}) {
            for (byte[] datagram : captured.get(offset)) {
                FecPacket theirs = FecPacket.decode(Unpooled.wrappedBuffer(datagram));
                assertThat(theirs).isNotNull();
                assertThat(theirs.row()).as("row FEC on P+4, column FEC on P+2").isEqualTo(offset == 4);
                FecPacket mine = ours.get(key(theirs));
                assertThat(mine).as("Press made the packet ffmpeg sent for " + key(theirs)).isNotNull();
                assertThat(mine).usingRecursiveComparison().ignoringFields("sequenceNumber", "payload")
                        .isEqualTo(theirs);
                assertThat(ByteBufUtil.getBytes(mine.payload())).as(key(theirs))
                        .containsExactly(ByteBufUtil.getBytes(theirs.payload()));
                compared++;
            }
        }
        assertThat(compared).isGreaterThan(300);
    }

    // -------------------------------------------------------------------------

    private record Result(int dropped, ReceiverStats stats, TsStreamStats health, int frames) {
    }

    /** ffmpeg sends media and FEC to a proxy that drops media per {@link #DROPS} on its way to Press. */
    private Result receiveThroughLossyLink(boolean fec) throws Exception {
        String ffmpeg = Ffmpeg.binary();
        Path fixture = Ffmpeg.fixture();
        RtpReceiver receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0))
                .withFec(fec).withLatency(Duration.ofMillis(400)));
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        receiver.onData(payload -> {
            synchronized (received) {
                received.writeBytes(ByteBufUtil.getBytes(payload));
            }
            payload.release();
        });
        int target = receiver.localAddress().getPort();
        int proxyBase = freePortRun();
        AtomicInteger mediaIndex = new AtomicInteger();
        AtomicInteger dropped = new AtomicInteger();
        List<Forwarder> proxy = List.of(
                new Forwarder(proxyBase, target, () -> {
                    if (DROPS.test(mediaIndex.getAndIncrement())) {
                        dropped.incrementAndGet();
                        return false;
                    }
                    return true;
                }),
                new Forwarder(proxyBase + 1, target + 1, () -> true),
                new Forwarder(proxyBase + 2, target + 2, () -> true),
                new Forwarder(proxyBase + 4, target + 4, () -> true));
        try {
            try (Ffmpeg.Running sender = Ffmpeg.start(List.of(ffmpeg, "-v", "warning", "-re",
                    "-i", fixture.toString(), "-c", "copy", "-f", "rtp_mpegts",
                    "-fec", "prompeg=l=" + L + ":d=" + D, "rtp://127.0.0.1:" + proxyBase))) {
                assertThat(sender.waitFor(Ffmpeg.FIXTURE_SECONDS + 20)).isTrue();
            }
            Thread.sleep(600);
        } finally {
            for (Forwarder forwarder : proxy) {
                forwarder.close();
            }
            receiver.close();
        }

        byte[] ts;
        synchronized (received) {
            ts = received.toByteArray();
        }
        Path out = Files.createTempFile("press-fec-", ".ts");
        out.toFile().deleteOnExit();
        Files.write(out, ts);
        return new Result(dropped.get(), receiver.stats(), Ffmpeg.analyse(ts), Ffmpeg.decodableVideoFrames(out));
    }

    private static String key(FecPacket packet) {
        return (packet.row() ? "row@" : "column@") + packet.snBase();
    }

    /** A free port P with P+1 to P+4 free as well. */
    private static int freePortRun() throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            int port;
            try (DatagramSocket probe = new DatagramSocket(0, LOOPBACK)) {
                port = probe.getLocalPort();
            }
            if (port > 0 && port < 65530 && Arrays.stream(new int[]{0, 1, 2, 3, 4}).allMatch(o -> canBind(port + o))) {
                return port;
            }
        }
        throw new IOException("no run of five free ports");
    }

    private static boolean canBind(int port) {
        try (DatagramSocket socket = new DatagramSocket(port, LOOPBACK)) {
            return true;
        } catch (SocketException e) {
            return false;
        }
    }

    /** Records every datagram arriving on one port. */
    private static final class Capture implements AutoCloseable {
        private final DatagramSocket socket;
        private final Thread thread;

        Capture(int port, List<byte[]> into) throws SocketException {
            socket = new DatagramSocket(port, LOOPBACK);
            socket.setReceiveBufferSize(8 << 20);
            thread = new Thread(() -> {
                byte[] buffer = new byte[2048];
                while (!socket.isClosed()) {
                    DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
                    try {
                        socket.receive(datagram);
                        into.add(Arrays.copyOf(buffer, datagram.getLength()));
                    } catch (IOException e) {
                        return;
                    }
                }
            }, "capture-" + port);
            thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            socket.close();
            thread.join();
        }
    }

    /** Forwards one port to another, dropping what the gate refuses. */
    private static final class Forwarder implements AutoCloseable {
        private final DatagramSocket socket;
        private final Thread thread;

        Forwarder(int port, int to, java.util.function.BooleanSupplier gate) throws SocketException {
            socket = new DatagramSocket(port, LOOPBACK);
            socket.setReceiveBufferSize(8 << 20);
            thread = new Thread(() -> {
                byte[] buffer = new byte[2048];
                try (DatagramSocket out = new DatagramSocket(0, LOOPBACK)) {
                    while (!socket.isClosed()) {
                        DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
                        socket.receive(datagram);
                        if (gate.getAsBoolean()) {
                            out.send(new DatagramPacket(buffer, datagram.getLength(), LOOPBACK, to));
                        }
                    }
                } catch (IOException e) {
                    // closed
                }
            }, "forward-" + port);
            thread.start();
        }

        @Override
        public void close() throws InterruptedException {
            socket.close();
            thread.join();
        }
    }
}