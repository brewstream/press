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
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.brewstream.press.net.RtpSenderTest.tsBytes;

class FanOutTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable resource : resources.reversed()) {
            resource.close();
        }
    }

    @Test
    void copiesEachPayloadToEveryDestinationAndPassesItOn() {
        FanOut fanOut = new FanOut();
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        fanOut.add("a", collect(a));
        fanOut.add("b", collect(b));
        List<String> downstream = new ArrayList<>();
        EmbeddedChannel channel = new EmbeddedChannel(fanOut, new SimpleChannelInboundHandler<ByteBuf>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                downstream.add(msg.toString(java.nio.charset.StandardCharsets.US_ASCII));
            }
        });
        ByteBuf payload = Unpooled.copiedBuffer("ts", java.nio.charset.StandardCharsets.US_ASCII);

        channel.writeInbound(payload);

        assertThat(a).containsExactly("ts");
        assertThat(b).containsExactly("ts");
        assertThat(downstream).containsExactly("ts");
        assertThat(payload.refCnt()).as("every reference released").isZero();
    }

    @Test
    void destinationsComeAndGoWhileTheStreamRuns() {
        FanOut fanOut = new FanOut();
        List<String> a = new ArrayList<>();
        List<String> b = new ArrayList<>();
        EmbeddedChannel channel = new EmbeddedChannel(fanOut);
        fanOut.add("a", collect(a));

        channel.writeInbound(text("1"));
        fanOut.add("b", collect(b));
        channel.writeInbound(text("2"));
        assertThat(fanOut.remove("a")).isTrue();
        channel.writeInbound(text("3"));

        assertThat(a).containsExactly("1", "2");
        assertThat(b).containsExactly("2", "3");
        assertThat(fanOut.destinations()).containsExactly("b");
        channel.finishAndReleaseAll();
    }

    @Test
    void oneFailingDestinationDoesNotStopTheOthers() {
        FanOut fanOut = new FanOut();
        List<String> good = new ArrayList<>();
        fanOut.add("bad", payload -> {
            payload.release();
            throw new IllegalStateException("boom");
        });
        fanOut.add("good", collect(good));
        EmbeddedChannel channel = new EmbeddedChannel(fanOut);

        channel.writeInbound(text("x"));

        assertThat(good).containsExactly("x");
        channel.finishAndReleaseAll();
    }

    /**
     * The relay case end to end: one RTP stream in, received by Press, sent on
     * by Press to three receivers, one of them added while the stream runs.
     */
    @Test
    void relaysOneRtpStreamToSeveralReceivers() throws Exception {
        FanOut fanOut = new FanOut();
        RtpReceiver ingest = track(RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0)),
                pipeline -> pipeline.addLast(fanOut)));
        ByteArrayOutputStream[] outputs = new ByteArrayOutputStream[3];
        for (int i = 0; i < 3; i++) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            outputs[i] = out;
            RtpReceiver output = track(RtpReceiver.bind(RtpReceiverConfig.unicast(new InetSocketAddress(LOOPBACK, 0))));
            output.onData(payload -> {
                synchronized (out) {
                    out.writeBytes(ByteBufUtil.getBytes(payload));
                }
                payload.release();
            });
            if (i < 2) {
                fanOut.add("out" + i, track(RtpSender.connect(RtpSenderConfig.to(output.localAddress())))::write);
            } else {
                lateOutput = output; // joins halfway through
            }
        }
        RtpSender source = track(RtpSender.connect(RtpSenderConfig.to(ingest.localAddress())));
        byte[] first = tsBytes(7 * 20);
        byte[] second = tsBytes(7 * 30);

        source.write(Unpooled.wrappedBuffer(first));
        awaitSize(outputs[0], first.length);
        fanOut.add("late", track(RtpSender.connect(RtpSenderConfig.to(lateOutput.localAddress())))::write);
        source.write(Unpooled.wrappedBuffer(second));

        byte[] both = new byte[first.length + second.length];
        System.arraycopy(first, 0, both, 0, first.length);
        System.arraycopy(second, 0, both, first.length, second.length);
        awaitSize(outputs[0], both.length);
        awaitSize(outputs[1], both.length);
        awaitSize(outputs[2], second.length);
        assertThat(bytes(outputs[0])).isEqualTo(both);
        assertThat(bytes(outputs[1])).isEqualTo(both);
        assertThat(bytes(outputs[2])).as("the late destination gets the stream from when it joined")
                .isEqualTo(second);
    }

    private RtpReceiver lateOutput;

    private <T extends AutoCloseable> T track(T resource) {
        resources.add(resource);
        return resource;
    }

    private static java.util.function.Consumer<ByteBuf> collect(List<String> into) {
        return payload -> {
            into.add(payload.toString(java.nio.charset.StandardCharsets.US_ASCII));
            payload.release();
        };
    }

    private static ByteBuf text(String text) {
        return Unpooled.copiedBuffer(text, java.nio.charset.StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(ByteArrayOutputStream out) {
        synchronized (out) {
            return out.toByteArray();
        }
    }

    private static void awaitSize(ByteArrayOutputStream out, int size) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (bytes(out).length < size) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("got " + bytes(out).length + " of " + size + " bytes");
            }
            Thread.sleep(10);
        }
    }
}