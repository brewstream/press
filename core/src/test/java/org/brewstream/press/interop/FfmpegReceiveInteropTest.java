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

import io.netty.buffer.ByteBufUtil;
import org.brewstream.grind.TsStreamStats;
import org.brewstream.press.net.ReceiverStats;
import org.brewstream.press.net.RtpReceiver;
import org.brewstream.press.net.RtpReceiverConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ffmpeg's RTP/MPEG-TS output (its {@code rtp_mpegts} muxer, RFC 2250 payload
 * type 33) received by Press. ffmpeg remuxes on the way out, so the bytes that
 * arrive are not the fixture's bytes. The checks are on what a receiver owes:
 * a transport stream with no continuity errors, every frame decodable, and the
 * sender's RTCP reports understood.
 */
@Tag("interop")
class FfmpegReceiveInteropTest {

    private RtpReceiver receiver;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (receiver != null) {
            receiver.close();
        }
    }

    @Test
    void receivesFfmpegsRtpMpegTsIntact() throws Exception {
        String ffmpeg = Ffmpeg.binary();
        Path fixture = Ffmpeg.fixture();
        receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)));
        ByteArrayOutputStream received = new ByteArrayOutputStream();
        receiver.onData(payload -> {
            synchronized (received) {
                received.writeBytes(ByteBufUtil.getBytes(payload));
            }
            payload.release();
        });
        int port = receiver.localAddress().getPort();

        try (Ffmpeg.Running sender = Ffmpeg.start(List.of(ffmpeg, "-v", "warning", "-re", "-i", fixture.toString(),
                "-c", "copy", "-f", "rtp_mpegts", "rtp://127.0.0.1:" + port))) {
            assertThat(sender.waitFor(Ffmpeg.FIXTURE_SECONDS + 20)).as("ffmpeg finished").isTrue();
            assertThat(sender.process().exitValue()).as(sender.logText()).isZero();
        }
        Thread.sleep(300); // let the last packets clear the reorder buffer
        receiver.close();

        ReceiverStats stats = receiver.stats();
        assertThat(stats.packetsLost()).isZero();
        assertThat(stats.packetsInvalid()).isZero();
        assertThat(stats.senderReports()).as("ffmpeg's RTCP sender reports").isPositive();

        byte[] ts;
        synchronized (received) {
            ts = received.toByteArray();
        }
        assertThat(ts.length).isEqualTo(stats.bytesDelivered());
        TsStreamStats health = Ffmpeg.analyse(ts);
        assertThat(health.continuityErrors()).isZero();
        assertThat(health.syncLosses()).isZero();
        assertThat(health.packets()).isPositive();

        Path out = Files.createTempFile("press-received-", ".ts");
        out.toFile().deleteOnExit();
        Files.write(out, ts);
        assertThat(Ffmpeg.decodableVideoFrames(out)).isEqualTo(Ffmpeg.FIXTURE_FRAMES);
    }
}