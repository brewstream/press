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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.ScheduledFuture;
import org.brewstream.press.fec.FecDecoder;
import org.brewstream.press.packet.FecPacket;
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
 * every five seconds or so. With FEC on, lost packets are rebuilt from SMPTE
 * 2022-1 row and column FEC before the gap they left is given up on.
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

    /** Media packets of history kept for FEC: two of the largest matrices 2022-1 allows, with room to spare. */
    private static final int FEC_HISTORY = 512;

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
    private DatagramChannel fecColumn;
    private DatagramChannel fecRow;
    private ChannelHandlerContext mediaContext;
    private ScheduledFuture<?> tick;
    private volatile boolean closed;
    private volatile ReceiverStats finalStats;

    // Everything below is owned by the event loop.
    private final SequenceTracker sequence = new SequenceTracker();
    private final JitterEstimator jitter = new JitterEstimator(MPEG_TS_CLOCK_RATE);
    private final ReorderBuffer reorder;
    private final FecDecoder fec;
    private boolean warnedLateRecovery;
    /** Recent recoveries by extended sequence number, to notice the original turning up after all. */
    private final long[] recoveredSeq = new long[FEC_HISTORY];
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
    private long packetsRecovered;
    private long packetsRecoveredLate;

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
        java.util.Arrays.fill(recoveredSeq, Long.MIN_VALUE);
        this.fec = config.fec()
                ? new FecDecoder(FEC_HISTORY, this::onRecovered, io.netty.buffer.PooledByteBufAllocator.DEFAULT)
                : null;
    }

    /** Binds a receiver on its own event loop group, shut down when the receiver closes. */
    public static RtpReceiver bind(RtpReceiverConfig config) throws InterruptedException {
        return bind(config, PressTransport.owned(), pipeline -> { });
    }

    /**
     * Binds a receiver whose pipeline is set up before the first packet is read,
     * so no packet can arrive ahead of the handlers meant to see it.
     *
     * <pre>{@code
     * RtpReceiver.bind(config, pipeline -> pipeline.addLast(new MpegTsDecoder(analyzer)));
     * }</pre>
     *
     * @param initializer runs once, on the receiver's event loop, before reading starts
     */
    public static RtpReceiver bind(RtpReceiverConfig config, Consumer<ChannelPipeline> initializer)
            throws InterruptedException {
        return bind(config, PressTransport.owned(), initializer);
    }

    /** Binds a receiver on the given transport. A shared group is never shut down by Press. */
    public static RtpReceiver bind(RtpReceiverConfig config, PressTransport transport) throws InterruptedException {
        return bind(config, transport, pipeline -> { });
    }

    /** Binds a receiver on the given transport, with its pipeline set up before reading starts. */
    public static RtpReceiver bind(RtpReceiverConfig config, PressTransport transport,
            Consumer<ChannelPipeline> initializer) throws InterruptedException {
        RtpReceiver receiver = new RtpReceiver(config, transport);
        try {
            receiver.open();
            receiver.loop.submit(() -> {
                initializer.accept(receiver.media.pipeline());
                receiver.media.config().setAutoRead(true);
            }).sync();
        } catch (InterruptedException | RuntimeException e) {
            receiver.close();
            throw e;
        }
        return receiver;
    }

    /**
     * The media socket's pipeline. Handlers added after Press's receive in-order
     * payload {@link ByteBuf}s and must release them.
     *
     * <p>Reading has already started by the time {@code bind} returns, and Netty
     * adds a handler from another thread asynchronously, so packets that arrive
     * in the meantime pass it by. To see a stream from its first packet, add
     * handlers in the initializer of {@link #bind(RtpReceiverConfig, Consumer)}.
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
     * Every packet delivered after this returns reaches it, because the handler
     * is added on the event loop and this waits until it is in place.
     */
    public void onData(Consumer<ByteBuf> handler) throws InterruptedException {
        ChannelHandler tail = new SimpleChannelInboundHandler<ByteBuf>(false) {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                handler.accept(payload);
            }
        };
        if (loop.inEventLoop()) {
            pipeline().addLast(tail);
        } else {
            loop.submit(() -> pipeline().addLast(tail)).sync();
        }
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
                    if (fec != null) {
                        fec.reset();
                    }
                    sendReport(true);
                    finalStats = snapshot();
                }).sync();
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // The group was shut down under us; nothing left to flush on.
            }
        }
        closeCompanions();
        Channels.closeAndAwaitRelease(media);
        if (transport.shutdownWithOwner()) {
            transport.eventLoopGroup().shutdownGracefully(0, 2, TimeUnit.SECONDS).sync();
        }
    }

    // --- binding -------------------------------------------------------------

    private void open() throws InterruptedException {
        int basePort = config.bindAddress().getPort();
        if (basePort != 0 || !config.rtcp() && !config.fec()) {
            bindAt(basePort);
        } else {
            bindEphemeral();
        }
        nextReportNanos = System.nanoTime() + reportInterval();
        long tickNanos = Math.clamp(config.latency().toNanos() / 4, TimeUnit.MILLISECONDS.toNanos(1),
                TimeUnit.MILLISECONDS.toNanos(10));
        tick = loop.scheduleAtFixedRate(this::onTick, tickNanos, tickNanos, TimeUnit.NANOSECONDS);
    }

    /** Port 0 with RTCP or FEC: find a base P where the ports above it are free too. */
    private void bindEphemeral() throws InterruptedException {
        for (int attempt = 0; attempt < EPHEMERAL_ATTEMPTS; attempt++) {
            media = bindChannel(0, new MediaHandler());
            int port = media.localAddress().getPort();
            if (port <= 65531) {
                try {
                    bindCompanions(port);
                    return;
                } catch (Channels.BindFailed e) {
                    // One of the ports above is taken; try another P.
                }
            }
            closeCompanions();
            Channels.closeAndAwaitRelease(media);
            media = null;
        }
        throw new IllegalStateException("no free base port with free RTCP and FEC ports above it after "
                + EPHEMERAL_ATTEMPTS + " attempts");
    }

    private void bindAt(int basePort) throws InterruptedException {
        media = bindChannel(basePort, new MediaHandler());
        bindCompanions(media.localAddress().getPort());
    }

    private void bindCompanions(int basePort) throws InterruptedException {
        if (config.rtcp()) {
            rtcp = bindChannel(basePort + 1, new RtcpHandler());
        }
        if (config.fec()) {
            fecColumn = bindChannel(basePort + 2, new FecHandler());
            fecRow = bindChannel(basePort + 4, new FecHandler());
        }
    }

    private void closeCompanions() throws InterruptedException {
        Channels.closeAndAwaitRelease(rtcp);
        Channels.closeAndAwaitRelease(fecColumn);
        Channels.closeAndAwaitRelease(fecRow);
        rtcp = null;
        fecColumn = null;
        fecRow = null;
    }

    private DatagramChannel bindChannel(int port, ChannelInboundHandlerAdapter handler) throws InterruptedException {
        // The media socket starts paused, and reading begins once bind has run the
        // pipeline initializer. RTCP and FEC sockets have no user handlers to wait for.
        boolean paused = handler instanceof MediaHandler;
        boolean multicast = config.multicastGroup() != null;
        InetSocketAddress local = new InetSocketAddress(config.bindAddress().getAddress(), port);
        ChannelFuture bound = transport.bootstrap(loop, multicast ? config.multicastGroup() : null)
                .option(ChannelOption.SO_RCVBUF, config.receiveBufferBytes())
                .option(ChannelOption.SO_REUSEADDR, multicast)
                .option(ChannelOption.AUTO_READ, !paused)
                .handler(handler)
                .bind(local)
                .await();
        if (!bound.isSuccess()) {
            throw new Channels.BindFailed(local, bound.cause());
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
            if (fec != null) {
                fec.reset();
            }
            sourceChanges++;
            long ssrc = sourceSsrc;
            notifyListeners(l -> l.onSourceChanged(this, ssrc, ssrc, sender));
        }
        packetsReceived++;
        jitter.update(now, packet.timestamp());
        if (fec != null) {
            fec.onMedia(extended, packet); // keeps its own reference
        }

        switch (reorder.offer(extended, packet, now)) {
            case ACCEPTED -> { }
            case DUPLICATE -> {
                int slot = (int) extended & (FEC_HISTORY - 1);
                if (recoveredSeq[slot] == extended) {
                    // FEC got here first, from another socket; the packet was never lost.
                    recoveredSeq[slot] = Long.MIN_VALUE;
                    packetsRecovered--;
                } else {
                    packetsDuplicate++;
                }
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
        if (fec != null) {
            fec.reset();
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

    private void onRecovered(long extendedSeq, RtpPacket packet) {
        packetsRecovered++;
        switch (reorder.offer(extendedSeq, packet, System.nanoTime())) {
            case ACCEPTED -> {
                recoveredSeq[(int) extendedSeq & (FEC_HISTORY - 1)] = extendedSeq;
                notifyListeners(l -> l.onRecovered(this, extendedSeq));
            }
            case DUPLICATE -> packet.body().release();
            case LATE -> {
                packetsRecoveredLate++;
                packet.body().release();
                if (!warnedLateRecovery) {
                    warnedLateRecovery = true;
                    LOG.warning("FEC recovered a packet after delivery had given up on it; latency "
                            + config.latency().toMillis() + " ms is shorter than the FEC needs ("
                            + fec.columns() + "x" + fec.rows() + " matrix). Raise it to about two matrices of packets.");
                }
            }
        }
    }

    private final class FecHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof DatagramPacket datagram)) {
                ctx.fireChannelRead(msg);
                return;
            }
            try {
                FecPacket packet = FecPacket.decode(datagram.content());
                if (packet == null) {
                    return;
                }
                if (sourceSsrc == -1) {
                    packet.payload().release();
                    return;
                }
                fec.onFec(packet);
            } finally {
                datagram.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.log(Level.WARNING, "FEC socket error", cause);
        }
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
                senderReports, fec == null ? 0 : fec.fecPackets(), packetsRecovered, packetsRecoveredLate,
                fec == null ? 0 : fec.columns(), fec == null ? 0 : fec.rows());
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