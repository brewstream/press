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

import io.netty.buffer.Unpooled;
import org.brewstream.grind.TsStreamStats;
import org.brewstream.press.net.RtpSender;
import org.brewstream.press.net.RtpSenderConfig;
import org.brewstream.press.net.SenderStats;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Press sending to ffmpeg's RTP input ({@code -i rtp://...}), which recognises
 * payload type 33 as MPEG-TS without an SDP. ffmpeg spends the start of a
 * stream probing it and does not record what it probed, so it keeps fewer than
 * all the frames whoever sends. Fed by ffmpeg's own {@code rtp_mpegts} output
 * in the same arrangement, it kept 123 of 150. The bar here is the same kind:
 * most frames, no decode errors, no continuity errors.
 */
@Tag("interop")
class FfmpegSendInteropTest {

    @Test
    void ffmpegReceivesWhatPressSends() throws Exception {
        String ffmpeg = Ffmpeg.binary();
        byte[] fixture = Files.readAllBytes(Ffmpeg.fixture());
        int port = freePortPair();
        Path out = Files.createTempFile("press-to-ffmpeg-", ".ts");
        out.toFile().deleteOnExit();

        SenderStats stats;
        try (Ffmpeg.Running receiver = Ffmpeg.start(List.of(ffmpeg, "-v", "warning", "-y",
                "-i", "rtp://127.0.0.1:" + port, "-c", "copy", "-f", "mpegts", out.toString()))) {
            // ffmpeg binds its port with address reuse, so a probe cannot tell when it is
            // listening. Give it time to start; anything sent before then is simply missed.
            Thread.sleep(1500);
            RtpSender sender = RtpSender.connect(RtpSenderConfig.to(new InetSocketAddress(
                    InetAddress.getLoopbackAddress(), port)));
            try {
                sendInRealTime(sender, fixture);
                Thread.sleep(1000);
            } finally {
                sender.close();
            }
            stats = sender.stats();
            assertThat(receiver.quit(10)).as("ffmpeg stopped").isTrue();
        }

        int frames = Ffmpeg.decodableVideoFrames(out);
        assertThat(frames).as("frames ffmpeg recorded").isBetween(Ffmpeg.FIXTURE_FRAMES - 50, Ffmpeg.FIXTURE_FRAMES);
        assertThat(Ffmpeg.decodeErrors(out)).isEmpty();
        TsStreamStats health = Ffmpeg.analyse(Files.readAllBytes(out));
        assertThat(health.continuityErrors()).isZero();
        assertThat(stats.packetsSent()).isEqualTo((fixture.length + 1315) / 1316);
    }

    /** Writes the stream at its own bitrate, in 1316-byte pieces, as a live source would. */
    private static void sendInRealTime(RtpSender sender, byte[] ts) throws InterruptedException {
        long bytesPerSecond = ts.length / Ffmpeg.FIXTURE_SECONDS;
        long start = System.nanoTime();
        for (int at = 0; at < ts.length; at += 1316) {
            int size = Math.min(1316, ts.length - at);
            sender.write(Unpooled.wrappedBuffer(ts, at, size));
            long due = start + at * 1_000_000_000L / bytesPerSecond;
            long wait = due - System.nanoTime();
            if (wait > 1_000_000) {
                Thread.sleep(wait / 1_000_000);
            }
        }
        sender.flush();
    }

    private static int freePortPair() throws Exception {
        for (int attempt = 0; attempt < 50; attempt++) {
            int port;
            try (DatagramSocket probe = new DatagramSocket(0, InetAddress.getLoopbackAddress())) {
                port = probe.getLocalPort() & ~1;
            }
            try (DatagramSocket a = new DatagramSocket(port, InetAddress.getLoopbackAddress());
                    DatagramSocket b = new DatagramSocket(port + 1, InetAddress.getLoopbackAddress())) {
                return port;
            } catch (SocketException e) {
                // try another
            }
        }
        throw new AssertionError("no free port pair");
    }
}