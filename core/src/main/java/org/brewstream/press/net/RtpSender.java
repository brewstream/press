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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.concurrent.ScheduledFuture;
import org.brewstream.press.fec.FecEncoder;
import org.brewstream.press.packet.FecPacket;
import org.brewstream.press.packet.NtpTime;
import org.brewstream.press.packet.RtcpPacket;
import org.brewstream.press.packet.RtpPacket;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sends a transport stream over RTP to one destination, unicast or multicast,
 * optionally with SMPTE 2022-1 FEC.
 *
 * <pre>{@code
 * RtpSender sender = RtpSender.connect(RtpSenderConfig.to(new InetSocketAddress("10.0.0.9", 5000)).withFec(5, 5));
 * sender.write(tsBytes);   // any amount; takes ownership
 * }</pre>
 *
 * <p>{@link #write} accepts transport stream bytes in any amounts, from any
 * thread, and packs them into RTP packets of {@code tsPacketsPerDatagram} × 188
 * bytes (1316 by default). A packet goes out as soon as it is full. {@link
 * #flush} sends a partial one. Sequence numbers start at random, as RFC 3550
 * asks. Timestamps are the 90 kHz time each packet is sent, which is what RFC
 * 2250 specifies for MPEG-TS.
 *
 * <p><b>Pacing is the caller's.</b> Packets leave when they are full, so a
 * live source (a capture, a Roast connection, a {@link RtpReceiver}) produces a
 * live-paced stream. A file written as fast as it can be read goes out as fast
 * as it can be read.
 *
 * <p><b>RTCP.</b> A sender report every five seconds or so (±50%, RFC 3550
 * §6.3.1) to the destination's port + 1, and BYE on close. Receiver reports
 * arriving on the local RTCP port update {@link #stats()} with the receiver's
 * loss and jitter, and the round-trip time worked out from them.
 *
 * <p><b>Threading.</b> One event loop owns the sender's state; {@code write}
 * from another thread is handed to it, in order.
 */
public final class RtpSender implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RtpSender.class.getName());

    private static final long RTCP_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int TS_PACKET = 188;
    private static final long CLOCK_RATE = 90_000;
    private static final int EPHEMERAL_ATTEMPTS = 32;

    private final RtpSenderConfig config;
    private final PressTransport transport;
    private final EventLoop loop;
    private final List<RtpSenderListener> listeners = new CopyOnWriteArrayList<>();
    private final long ssrc = ThreadLocalRandom.current().nextLong(1, 1L << 32);
    private final String cname = "press-" + Long.toHexString(ssrc);
    private final int packetBytes;
    private final long startNanos = System.nanoTime();
    private final long timestampBase = ThreadLocalRandom.current().nextLong(1L << 32);

    private DatagramChannel media;
    private DatagramChannel rtcp;
    private ScheduledFuture<?> reportTimer;
    private volatile boolean closed;
    private volatile SenderStats finalStats;

    private final FecEncoder fec;
    private ByteBuf pending;
    private int sequence = ThreadLocalRandom.current().nextInt(1 << 16);
    private long packetsSent;
    private long bytesSent;
    private long fecPacketsSent;
    private long receiverReports;
    private InetSocketAddress lastReceiver;
    private int lastFractionLost;
    private long lastCumulativeLost;
    private long lastJitter;
    private long lastRttMicros = -1;

    private RtpSender(RtpSenderConfig config, PressTransport transport) {
        this.config = config;
        this.transport = transport;
        this.loop = transport.eventLoopGroup().next();
        this.packetBytes = config.tsPacketsPerDatagram() * TS_PACKET;
        this.fec = config.fec()
                ? new FecEncoder(config.fecColumns(), config.fecRows(), new FecEncoder.Sink() {
                    @Override
                    public void column(FecPacket packet) {
                        sendFec(packet, 2);
                    }

                    @Override
                    public void row(FecPacket packet) {
                        sendFec(packet, 4);
                    }
                }, io.netty.buffer.PooledByteBufAllocator.DEFAULT)
                : null;
    }

    /** Opens a sender on its own event loop group, shut down when the sender closes. */
    public static RtpSender connect(RtpSenderConfig config) throws InterruptedException {
        return connect(config, PressTransport.owned());
    }

    /** Opens a sender on the given transport. A shared group is never shut down by Press. */
    public static RtpSender connect(RtpSenderConfig config, PressTransport transport) throws InterruptedException {
        RtpSender sender = new RtpSender(config, transport);
        try {
            sender.open();
        } catch (InterruptedException | RuntimeException e) {
            sender.close();
            throw e;
        }
        return sender;
    }

    /**
     * Queues transport stream bytes for sending. Takes ownership of {@code ts}.
     * Safe from any thread; writes from one thread are sent in order. After close,
     * the bytes are released and nothing is sent.
     */
    public void write(ByteBuf ts) {
        if (closed) {
            ts.release();
            return;
        }
        if (loop.inEventLoop()) {
            packetise(ts);
        } else {
            loop.execute(() -> packetise(ts));
        }
    }

    /**
     * Sends the whole TS packets waiting for a full RTP packet now, as a shorter
     * one. An incomplete TS packet at the end stays for the next write: RFC 2250
     * §2 allows only whole transport packets in a payload.
     */
    public void flush() {
        if (loop.inEventLoop()) {
            sendWholeTsPackets();
        } else {
            loop.execute(this::sendWholeTsPackets);
        }
    }

    public void addListener(RtpSenderListener listener) {
        listeners.add(listener);
    }

    /** The local media address. RTCP is on the port above it. */
    public InetSocketAddress localAddress() {
        return media.localAddress();
    }

    public long ssrc() {
        return ssrc;
    }

    /** A snapshot of the counters. Safe from any thread, and after close. */
    public SenderStats stats() {
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
            // The loop is gone.
        }
        return finalStats;
    }

    /**
     * Sends the whole TS packets still pending and a final SR with BYE, then
     * closes. An incomplete TS packet at the very end is discarded, since sending
     * it would break RFC 2250.
     *
     * <p>Called from any other thread, this returns once the ports can be bound
     * again. Called on the sender's own event loop it cannot wait for that without
     * blocking the loop it would wait on, so the sockets close as soon as the loop
     * is free and this returns at once.
     */
    @Override
    public void close() throws InterruptedException {
        if (closed) {
            return;
        }
        closed = true;
        if (loop.inEventLoop()) {
            finishOnLoop();
            if (rtcp != null) {
                rtcp.close();
            }
            if (media != null) {
                media.close();
            }
            if (transport.shutdownWithOwner()) {
                transport.eventLoopGroup().shutdownGracefully(0, 2, TimeUnit.SECONDS);
            }
            return;
        }
        if (!loop.isShuttingDown()) {
            try {
                loop.submit(this::finishOnLoop).sync();
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // The group was shut down under us.
            }
        }
        Channels.closeAndAwaitRelease(rtcp);
        Channels.closeAndAwaitRelease(media);
        if (transport.shutdownWithOwner()) {
            transport.eventLoopGroup().shutdownGracefully(0, 2, TimeUnit.SECONDS).sync();
        }
    }

    /** The part of closing that touches sender state, so runs on the loop. */
    private void finishOnLoop() {
        if (reportTimer != null) {
            reportTimer.cancel(false);
        }
        sendWholeTsPackets();
        if (pending != null) {
            LOG.fine(() -> "discarding " + pending.readableBytes() + " bytes of an incomplete final TS packet");
            pending.release();
            pending = null;
        }
        sendReport(true);
        if (fec != null) {
            fec.close();
        }
        finalStats = snapshot();
    }

    // --- binding -------------------------------------------------------------

    private void open() throws InterruptedException {
        int localPort = config.localAddress().getPort();
        if (localPort != 0 || !config.rtcp()) {
            media = bind(localPort, null);
            if (config.rtcp()) {
                rtcp = bind(media.localAddress().getPort() + 1, new RtcpHandler());
            }
        } else {
            bindEphemeralPair();
        }
        if (config.rtcp()) {
            reportTimer = loop.schedule(this::onReportTimer, reportInterval() / 2, TimeUnit.NANOSECONDS);
        }
    }

    private void bindEphemeralPair() throws InterruptedException {
        for (int attempt = 0; attempt < EPHEMERAL_ATTEMPTS; attempt++) {
            media = bind(0, null);
            int port = media.localAddress().getPort();
            if (port < 65535) {
                try {
                    rtcp = bind(port + 1, new RtcpHandler());
                    return;
                } catch (Channels.BindFailed e) {
                    // Q+1 is taken; try another Q.
                }
            }
            Channels.closeAndAwaitRelease(media);
            media = null;
        }
        throw new IllegalStateException("no free local port pair after " + EPHEMERAL_ATTEMPTS + " attempts");
    }

    private DatagramChannel bind(int port, ChannelInboundHandlerAdapter handler) throws InterruptedException {
        boolean multicast = config.destination().getAddress().isMulticastAddress();
        InetSocketAddress local = new InetSocketAddress(config.localAddress().getAddress(), port);
        var bootstrap = transport.bootstrap(loop, multicast ? config.destination().getAddress() : null)
                .option(ChannelOption.SO_SNDBUF, config.sendBufferBytes())
                .handler(handler != null ? handler : new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                        io.netty.util.ReferenceCountUtil.release(msg); // nothing is expected on the media port
                    }
                });
        if (multicast && config.ttl() >= 0) {
            bootstrap.option(ChannelOption.IP_MULTICAST_TTL, config.ttl());
        }
        if (multicast && config.networkInterface() != null) {
            bootstrap.option(ChannelOption.IP_MULTICAST_IF, config.networkInterface());
        }
        ChannelFuture bound = bootstrap.bind(local).await();
        if (!bound.isSuccess()) {
            throw new Channels.BindFailed(local, bound.cause());
        }
        return (DatagramChannel) bound.channel();
    }

    // --- media ---------------------------------------------------------------

    private void packetise(ByteBuf ts) {
        try {
            while (ts.isReadable()) {
                if (pending == null) {
                    pending = media.alloc().buffer(packetBytes);
                }
                int take = Math.min(ts.readableBytes(), packetBytes - pending.readableBytes());
                pending.writeBytes(ts, take);
                if (pending.readableBytes() == packetBytes) {
                    sendPending();
                }
            }
        } finally {
            ts.release();
        }
    }

    /** Sends the whole TS packets in {@link #pending}, keeping any incomplete one after them. */
    private void sendWholeTsPackets() {
        if (pending == null) {
            return;
        }
        int whole = pending.readableBytes() / TS_PACKET * TS_PACKET;
        if (whole == 0) {
            return;
        }
        ByteBuf rest = null;
        if (whole < pending.readableBytes()) {
            rest = media.alloc().buffer(packetBytes);
            rest.writeBytes(pending, pending.readerIndex() + whole, pending.readableBytes() - whole);
            pending.writerIndex(pending.readerIndex() + whole);
        }
        sendPending();
        pending = rest;
    }

    private void sendPending() {
        if (pending == null || !pending.isReadable()) {
            return;
        }
        ByteBuf payload = pending;
        pending = null;
        if (media == null || !media.isActive()) {
            payload.release();
            return;
        }
        RtpPacket packet = RtpPacket.of(false, config.payloadType(), sequence, timestampNow(), ssrc, payload);
        sequence = (sequence + 1) & 0xFFFF;
        packetsSent++;
        bytesSent += payload.readableBytes();
        media.writeAndFlush(new DatagramPacket(packet.encode(media.alloc()), config.destination()));
        if (fec != null) {
            fec.onMedia(packet);
        }
        payload.release();
    }

    private void sendFec(FecPacket packet, int portOffset) {
        try {
            if (media == null || !media.isActive()) {
                return;
            }
            InetSocketAddress to = new InetSocketAddress(config.destination().getAddress(),
                    config.destination().getPort() + portOffset);
            media.writeAndFlush(new DatagramPacket(packet.encode(media.alloc()), to));
            fecPacketsSent++;
        } finally {
            packet.payload().release();
        }
    }

    private long timestampNow() {
        long elapsedMicros = (System.nanoTime() - startNanos) / 1_000;
        return (timestampBase + elapsedMicros * CLOCK_RATE / 1_000_000) & 0xFFFF_FFFFL;
    }

    // --- RTCP ----------------------------------------------------------------

    private void onReportTimer() {
        sendReport(false);
        reportTimer = loop.schedule(this::onReportTimer, reportInterval(), TimeUnit.NANOSECONDS);
    }

    /** SR (or RR before anything was sent) with SDES CNAME, plus BYE when closing. */
    private void sendReport(boolean goodbye) {
        if (rtcp == null || !rtcp.isActive()) {
            return;
        }
        long ntp = NtpTime.now();
        RtcpPacket report = packetsSent > 0
                ? new RtcpPacket.SenderReport(ssrc, ntp, timestampNow(), packetsSent & 0xFFFF_FFFFL,
                        bytesSent & 0xFFFF_FFFFL, List.of())
                : new RtcpPacket.ReceiverReport(ssrc, List.of());
        List<RtcpPacket> compound = goodbye
                ? List.of(report, new RtcpPacket.SourceDescription(ssrc, cname), new RtcpPacket.Goodbye(List.of(ssrc)))
                : List.of(report, new RtcpPacket.SourceDescription(ssrc, cname));
        ByteBuf out = rtcp.alloc().buffer();
        RtcpPacket.encodeCompound(compound, out);
        InetSocketAddress to = new InetSocketAddress(config.destination().getAddress(),
                config.destination().getPort() + 1);
        rtcp.writeAndFlush(new DatagramPacket(out, to));
    }

    private final class RtcpHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (!(msg instanceof DatagramPacket datagram)) {
                ctx.fireChannelRead(msg);
                return;
            }
            try {
                onRtcp(datagram.content(), datagram.sender());
            } finally {
                datagram.release();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            LOG.log(Level.WARNING, "RTCP socket error", cause);
        }
    }

    private void onRtcp(ByteBuf datagram, InetSocketAddress from) {
        List<RtcpPacket> packets = RtcpPacket.decodeCompound(datagram);
        if (packets == null) {
            return;
        }
        for (RtcpPacket packet : packets) {
            List<RtcpPacket.ReportBlock> blocks = switch (packet) {
                case RtcpPacket.ReceiverReport rr -> rr.reports();
                case RtcpPacket.SenderReport sr -> sr.reports();
                default -> List.of();
            };
            for (RtcpPacket.ReportBlock block : blocks) {
                if (block.ssrc() == ssrc) {
                    onReport(block, from);
                }
            }
            if (packet instanceof RtcpPacket.Goodbye bye) {
                bye.ssrcs().forEach(id -> notifyListeners(l -> l.onGoodbye(this, from, id)));
            }
        }
    }

    /** RFC 3550 §6.4.1: RTT = now - LSR - DLSR, all in compact NTP units. */
    private void onReport(RtcpPacket.ReportBlock block, InetSocketAddress from) {
        receiverReports++;
        lastReceiver = from;
        lastFractionLost = block.fractionLost();
        lastCumulativeLost = block.cumulativeLost();
        lastJitter = block.jitter();
        long rtt = -1;
        if (block.lastSenderReport() != 0) {
            long compact = (NtpTime.compact(NtpTime.now()) - block.lastSenderReport()
                    - block.delaySinceLastSenderReport()) & 0xFFFF_FFFFL;
            if (compact < 0x8000_0000L) { // otherwise the clocks or the report are wrong
                rtt = NtpTime.compactToMicros(compact);
            }
        }
        lastRttMicros = rtt >= 0 ? rtt : lastRttMicros;
        long reportedRtt = rtt;
        notifyListeners(l -> l.onReceiverReport(this, from, block, reportedRtt));
    }

    private static long reportInterval() {
        return (long) (RTCP_INTERVAL_NANOS * (0.5 + ThreadLocalRandom.current().nextDouble()));
    }

    // --- misc ----------------------------------------------------------------

    private SenderStats snapshot() {
        return new SenderStats(ssrc, packetsSent, bytesSent, fecPacketsSent, receiverReports, lastReceiver,
                lastFractionLost / 256.0, lastCumulativeLost, lastJitter * 1_000_000 / CLOCK_RATE, lastRttMicros);
    }

    private void notifyListeners(Consumer<RtpSenderListener> event) {
        for (RtpSenderListener listener : listeners) {
            try {
                event.accept(listener);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "sender listener threw", e);
            }
        }
    }
}