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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.ScheduledFuture;
import org.brewstream.press.packet.NtpTime;
import org.brewstream.press.packet.RtcpPacket;
import org.brewstream.press.packet.RtpPacket;
import org.brewstream.press.recv.JitterEstimator;
import org.brewstream.press.recv.ReorderBuffer;
import org.brewstream.press.recv.SequenceTracker;

import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives one RTP stream carrying MPEG-TS and hands its payload, in order, to a
 * Netty pipeline.
 *
 * <pre>{@code
 * RtpReceiver receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(5000));
 * receiver.pipeline().addLast(new MpegTsDecoder(analyzer), new TsHealthHandler(analyzer));
 * }</pre>
 *
 * <p>The pipeline is the media socket's own. Press's handler sits at its head and
 * turns datagrams into in-order payload {@link ByteBuf}s, so handlers added after
 * it see the transport stream exactly as handlers on a Roast SRT connection do.
 * Each buffer is one packet's payload, normally seven 188-byte TS packets, and
 * the handler that consumes it releases it.
 *
 * <p><b>What happens on the way.</b> Packets are checked (RFC 3550 A.1), locked
 * to one SSRC, numbered past the 16-bit wrap, and reordered. A gap is waited on
 * for up to the configured latency, then given up as lost and reported to
 * {@link RtpReceiverListener#onLoss}. Interarrival jitter and the RFC 3550 loss
 * figures are kept, and with RTCP on, the receiver reports them to the sender
 * every five seconds or so.
 *
 * <p><b>Sources.</b> The first valid packet's SSRC is locked. Packets from any
 * other SSRC are counted and dropped while it is active. Once it has been
 * silent for {@link #SOURCE_SWITCH_SILENCE_MILLIS}, or has said BYE, the next
 * SSRC heard takes over: an encoder that restarts with a new SSRC is followed
 * without intervention, while two senders on one port do not interleave.
 *
 * <p><b>Threading.</b> All of a receiver's sockets share one event loop and its
 * state lives there, unsynchronised. Listeners are called on that loop.
 */
public final class RtpReceiver implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RtpReceiver.class.getName());

    /** How long a source must be silent before another SSRC may replace it. */
    public static final long SOURCE_SWITCH_SILENCE_MILLIS = 500;

    /** RFC 3550 §6.2's minimum report interval, randomised by ±50% per §6.3.1. */
    private static final long RTCP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

    /** Window of the reorder buffer, in packets: a few seconds of a busy stream. */
    private static final int REORDER_CAPACITY = 8192;

    private static final long MPEG_TS_CLOCK_RATE = 90_000;

    /** Attempts at finding a free base port when the configured one is 0. */
    private static final int EPHEMERAL_ATTEMPTS = 32;

    private final RtpReceiverConfig config;
    private final PressTransport transport;
    private final EventLoop loop;
    private final List<RtpReceiverListener> listeners = new CopyOnWriteArrayList<>();
    private final long ownSsrc = ThreadLocalRandom.current().nextLong(1, 1L << 32);
    private final String cname = "press-" + Long.toHexString(ownSsrc);

    private DatagramChannel media;
    private DatagramChannel rtcp;
    private ChannelHandlerContext mediaContext;
    private ScheduledFuture<?> tick;
    private volatile boolean closed;
    private volatile ReceiverStats finalStats;

    // Everything below is owned by the event loop.
    private final SequenceTracker sequence = new SequenceTracker();
    private final JitterEstimator jitter = new JitterEstimator(MPEG_TS_CLOCK_RATE);
    private final ReorderBuffer reorder;
    private long sourceSsrc = -1;
    private InetSocketAddress sourceAddress;
    private InetSocketAddress senderRtcpAddress;
    private long lastSourcePacketNanos;
    private boolean sourceSaidGoodbye;
    private long lastSenderReportCompact;
    private long lastSenderReportNanos;
    private long nextReportNanos;

    private long packetsReceived;
    private long packetsDelivered;
    private long packetsLost;
    private long packetsDuplicate;
    private long packetsLate;
    private long packetsInvalid;
    private long packetsForeign;
    private long bytesDelivered;
    private long sourceChanges;
    private long senderReports;

    private RtpReceiver(RtpReceiverConfig config, PressTransport transport) {
        this.config = config;
        this.transport = transport;
        this.loop = transport.eventLoopGroup().next();
        this.reorder = new ReorderBuffer(REORDER_CAPACITY, config.latency().toNanos(), new ReorderBuffer.Sink() {
            @Override
            public void deliver(long extendedSeq, RtpPacket packet) {
                onDeliver(packet);
            }

            @Override
            public void lost(long firstExtendedSeq, int count) {
                onLost(firstExtendedSeq, count);
            }
        });
    }

    /** Binds a receiver on its own event loop group, shut down when the receiver closes. */
    public static RtpReceiver bind(RtpReceiverConfig config) throws InterruptedException {
        return bind(config, PressTransport.owned());
    }

    /** Binds a receiver on the given transport. A shared group is never shut down by Press. */
    public static RtpReceiver bind(RtpReceiverConfig config, PressTransport transport) throws InterruptedException {
        RtpReceiver receiver = new RtpReceiver(config, transport);
        try {
            receiver.open();
        } catch (InterruptedException | RuntimeException e) {
            receiver.close();
            throw e;
        }
        return receiver;
    }

    /**
     * The media socket's pipeline. Add handlers with {@code addLast}; they receive
     * in-order payload {@link ByteBuf}s and must release them.
     */
    public ChannelPipeline pipeline() {
        return media.pipeline();
    }

    /** The media socket's channel. */
    public Channel channel() {
        return media;
    }

    /**
     * Calls {@code handler} with each in-order payload: a shortcut for a handler at
     * the end of the pipeline. The handler owns each buffer and must release it.
     */
    public void onData(Consumer<ByteBuf> handler) {
        pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>(false) {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                handler.accept(payload);
            }
        });
    }

    public void addListener(RtpReceiverListener listener) {
        listeners.add(listener);
    }

    /** The media socket's local address. Its port is the base port P. */
    public InetSocketAddress localAddress() {
        return media.localAddress();
    }

    /** A snapshot of the counters. Safe from any thread, and after close. */
    public ReceiverStats stats() {
        if (closed && finalStats != null) {
            return finalStats;
        }
        if (loop.inEventLoop()) {
            return snapshot();
        }
        try {
            return loop.submit(this::snapshot).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | java.util.concurrent.RejectedExecutionException e) {
            // The loop is gone; fall through to whatever was captured at close.
        }
        return finalStats;
    }

    /**
     * Stops receiving. Packets still waiting behind a gap are delivered first, a
     * BYE is sent if RTCP is on, and the ports are free when this returns.
     */
    @Override
    public void close() throws InterruptedException {
        if (closed) {
            return;
        }
        closed = true;
        if (!loop.isShuttingDown()) {
            try {
                loop.submit(() -> {
                    if (tick != null) {
                        tick.cancel(false);
                    }
                    reorder.flush();
                    sendReport(true);
                    finalStats = snapshot();
                }).sync();
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // The group was shut down under us; nothing left to flush on.
            }
        }
        Channels.closeAndAwaitRelease(rtcp);
        Channels.closeAndAwaitRelease(media);
        if (transport.shutdownWithOwner()) {
            transport.eventLoopGroup().shutdownGracefully(0, 2, TimeUnit.SECONDS).sync();
        }
    }

    // --- binding -------------------------------------------------------------

    private void open() throws InterruptedException {
        int basePort = config.bindAddress().getPort();
        if (basePort != 0 || !config.rtcp()) {
            bindAt(basePort);
        } else {
            bindEphemeral();
        }
        nextReportNanos = System.nanoTime() + reportInterval();
        long tickNanos = Math.clamp(config.latency().toNanos() / 4, TimeUnit.MILLISECONDS.toNanos(1),
                TimeUnit.MILLISECONDS.toNanos(10));
        tick = loop.scheduleAtFixedRate(this::onTick, tickNanos, tickNanos, TimeUnit.NANOSECONDS);
    }

    /** Port 0 with RTCP: find a base P where P+1 is free too. */
    private void bindEphemeral() throws InterruptedException {
        for (int attempt = 0; attempt < EPHEMERAL_ATTEMPTS; attempt++) {
            media = bindChannel(0, new MediaHandler());
            int port = media.localAddress().getPort();
            if (port < 65535) {
                try {
                    rtcp = bindChannel(port + 1, new RtcpHandler());
                    return;
                } catch (ChannelBindException e) {
                    // P+1 is taken; try another P.
                }
            }
            Channels.closeAndAwaitRelease(media);
            media = null;
        }
        throw new IllegalStateException("no free base port with a free RTCP port above it after "
                + EPHEMERAL_ATTEMPTS + " attempts");
    }

    private void bindAt(int basePort) throws InterruptedException {
        media = bindChannel(basePort, new MediaHandler());
        if (config.rtcp()) {
            rtcp = bindChannel(media.localAddress().getPort() + 1, new RtcpHandler());
        }
    }

    private DatagramChannel bindChannel(int port, ChannelInboundHandlerAdapter handler) throws InterruptedException {
        boolean multicast = config.multicastGroup() != null;
        InetSocketAddress local = new InetSocketAddress(config.bindAddress().getAddress(), port);
        ChannelFuture bound = transport.bootstrap(loop, multicast ? config.multicastGroup() : null)
                .option(ChannelOption.SO_RCVBUF, config.receiveBufferBytes())
                .option(ChannelOption.SO_REUSEADDR, multicast)
                .handler(handler)
                .bind(local)
                .await();
        if (!bound.isSuccess()) {
            throw new ChannelBindException(local, bound.cause());
        }
        DatagramChannel channel = (DatagramChannel) bound.channel();
        if (multicast) {
            try {
                NetworkInterface nic = config.networkInterface() != null
                        ? config.networkInterface()
                        : Channels.defaultMulticastInterface(config.multicastGroup());
                ChannelFuture joined = channel.joinGroup(config.multicastGroup(), nic, config.sourceFilter()).await();
                if (!joined.isSuccess()) {
                    throw new IllegalStateException("could not join " + config.multicastGroup().getHostAddress()
                            + " on " + nic.getName(), joined.cause());
                }
            } catch (java.net.SocketException | RuntimeException e) {
                Channels.closeAndAwaitRelease(channel);
                throw e instanceof RuntimeException runtime ? runtime : new IllegalStateException(e);
            }
        }
        return channel;
    }

    /** A port that could not be bound, kept distinct so the ephemeral search can retry. */
    static final class ChannelBindException extends IllegalStateException {
        ChannelBindException(InetSocketAddress address, Throwable cause) {
            super("could not bind " + address, cause);
        }
    }

    // --- media path ----------------------------------------------------------

    private final class MediaHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            mediaContext = ctx;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof DatagramPacket datagram)) {
                ctx.fireChannelRead(msg);
                return;
            }
            try {
                onMedia(datagram.content(), datagram.sender(), System.nanoTime());
            } finally {
                datagram.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.log(Level.WARNING, "media socket error", cause);
        }
    }

    private void onMedia(ByteBuf datagram, InetSocketAddress sender, long now) {
        RtpPacket packet = RtpPacket.decode(datagram);
        if (packet == null) {
            packetsInvalid++;
            return;
        }
        if (packet.ssrc() != sourceSsrc) {
            boolean sourceGone = sourceSsrc == -1 || sourceSaidGoodbye
                    || now - lastSourcePacketNanos > TimeUnit.MILLISECONDS.toNanos(SOURCE_SWITCH_SILENCE_MILLIS);
            if (!sourceGone) {
                packetsForeign++;
                packet.body().release();
                return;
            }
            changeSource(packet.ssrc(), sender);
        }
        lastSourcePacketNanos = now;
        sourceAddress = sender;

        long extended = sequence.update(packet.sequenceNumber());
        if (extended == SequenceTracker.INVALID) {
            packetsInvalid++;
            packet.body().release();
            return;
        }
        if (sequence.restarted()) {
            reorder.flush();
            reorder.reset();
            sourceChanges++;
            long ssrc = sourceSsrc;
            notifyListeners(l -> l.onSourceChanged(this, ssrc, ssrc, sender));
        }
        packetsReceived++;
        jitter.update(now, packet.timestamp());

        switch (reorder.offer(extended, packet, now)) {
            case ACCEPTED -> { }
            case DUPLICATE -> {
                packetsDuplicate++;
                packet.body().release();
            }
            case LATE -> {
                packetsLate++;
                packet.body().release();
            }
        }
    }

    private void changeSource(long ssrc, InetSocketAddress sender) {
        long previous = sourceSsrc;
        if (previous != -1) {
            reorder.flush();
            reorder.reset();
        }
        sequence.reset();
        jitter.reset();
        sourceSsrc = ssrc;
        sourceSaidGoodbye = false;
        senderRtcpAddress = null;
        lastSenderReportCompact = 0;
        sourceChanges++;
        notifyListeners(l -> l.onSourceChanged(this, previous, ssrc, sender));
    }

    private void onDeliver(RtpPacket packet) {
        ByteBuf payload = packet.payload();
        packetsDelivered++;
        bytesDelivered += payload.readableBytes();
        if (mediaContext == null) {
            packet.body().release();
            return;
        }
        // The payload slice shares the body's reference count, so whoever
        // consumes it releases the body.
        mediaContext.fireChannelRead(payload);
        mediaContext.fireChannelReadComplete();
    }

    private void onLost(long firstExtendedSeq, int count) {
        packetsLost += count;
        notifyListeners(l -> l.onLoss(this, firstExtendedSeq, count));
    }

    private void onTick() {
        long now = System.nanoTime();
        reorder.drain(now);
        if (now - nextReportNanos >= 0) {
            sendReport(false);
            nextReportNanos = now + reportInterval();
        }
    }

    // --- RTCP ----------------------------------------------------------------

    private final class RtcpHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof DatagramPacket datagram)) {
                ctx.fireChannelRead(msg);
                return;
            }
            try {
                onRtcp(datagram.content(), datagram.sender(), System.nanoTime());
            } finally {
                datagram.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.log(Level.WARNING, "RTCP socket error", cause);
        }
    }

    private void onRtcp(ByteBuf datagram, InetSocketAddress sender, long now) {
        List<RtcpPacket> packets = RtcpPacket.decodeCompound(datagram);
        if (packets == null) {
            return;
        }
        for (RtcpPacket packet : packets) {
            if (packet instanceof RtcpPacket.SenderReport report && report.ssrc() == sourceSsrc) {
                senderReports++;
                lastSenderReportCompact = report.compactNtp();
                lastSenderReportNanos = now;
                senderRtcpAddress = sender;
                notifyListeners(l -> l.onSenderReport(this, report));
            } else if (packet instanceof RtcpPacket.Goodbye bye && bye.ssrcs().contains(sourceSsrc)) {
                sourceSaidGoodbye = true;
                long ssrc = sourceSsrc;
                notifyListeners(l -> l.onGoodbye(this, ssrc));
            }
        }
    }

    /**
     * Sends RR and SDES, plus BYE when closing (RFC 3550 §6.1: a compound packet
     * starts with a report even when it carries a BYE). Goes to the address the
     * sender's reports came from, or failing that to the media source's port + 1.
     */
    private void sendReport(boolean goodbye) {
        if (rtcp == null || !rtcp.isActive() || sourceSsrc == -1) {
            return;
        }
        InetSocketAddress destination = senderRtcpAddress != null
                ? senderRtcpAddress
                : sourceAddress == null ? null
                : new InetSocketAddress(sourceAddress.getAddress(), sourceAddress.getPort() + 1);
        if (destination == null) {
            return;
        }
        long now = System.nanoTime();
        long delaySinceSr = lastSenderReportCompact == 0 ? 0 : NtpTime.nanosToCompact(now - lastSenderReportNanos);
        RtcpPacket.ReportBlock block = new RtcpPacket.ReportBlock(
                sourceSsrc,
                sequence.takeFractionLost(),
                sequence.cumulativeLost(),
                sequence.extendedMax() & 0xFFFF_FFFFL,
                jitter.jitter(),
                lastSenderReportCompact,
                delaySinceSr);
        List<RtcpPacket> compound = goodbye
                ? List.of(new RtcpPacket.ReceiverReport(ownSsrc, List.of(block)),
                        new RtcpPacket.SourceDescription(ownSsrc, cname),
                        new RtcpPacket.Goodbye(List.of(ownSsrc)))
                : List.of(new RtcpPacket.ReceiverReport(ownSsrc, List.of(block)),
                        new RtcpPacket.SourceDescription(ownSsrc, cname));
        ByteBuf out = rtcp.alloc().buffer();
        RtcpPacket.encodeCompound(compound, out);
        rtcp.writeAndFlush(new DatagramPacket(out, destination));
    }

    private static long reportInterval() {
        return (long) (RTCP_INTERVAL_NANOS * (0.5 + ThreadLocalRandom.current().nextDouble()));
    }

    // --- misc ----------------------------------------------------------------

    private ReceiverStats snapshot() {
        return new ReceiverStats(sourceSsrc, sourceAddress, packetsReceived, packetsDelivered, packetsLost,
                packetsDuplicate, packetsLate, packetsInvalid, packetsForeign, bytesDelivered,
                sourceSsrc == -1 ? 0 : sequence.cumulativeLost(), jitter.jitterMicros(), sourceChanges,
                senderReports);
    }

    private void notifyListeners(Consumer<RtpReceiverListener> event) {
        for (RtpReceiverListener listener : listeners) {
            try {
                event.accept(listener);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "receiver listener threw", e);
            }
        }
    }
}