# Press

Pure-Java RTP transport for MPEG-TS. Part of **BrewStream**: Press is the RTP
leg, as [Roast](https://github.com/brewstream/roast) is the SRT leg.

Press carries a transport stream over RTP the way broadcast contribution links
do: RFC 3550 RTP and RTCP, the RFC 2250 / SMPTE 2022-2 MPEG-TS payload, SMPTE
2022-1 forward error correction, unicast or multicast. A receiver puts packets
back in order, rebuilds lost ones from FEC, decides when a missing one is gone
for good, keeps the RFC 3550 statistics, and reports them to the sender over
RTCP. A sender packs a transport stream into RTP, with FEC if asked. Its data path is a Netty pipeline, so a stream received by Press is
inspected with [Grind](https://github.com/brewstream/grind) exactly as an SRT
stream received by Roast is.

**Status:** feature-complete for its first release, not yet released. See
[Roadmap](#roadmap) for what comes after.

## Requirements

Java 21 or newer. Netty 4.2 is the only runtime dependency.

## Receiving

```java
TsAnalyzer analyzer = new TsAnalyzer();
RtpReceiver receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(5000),
        pipeline -> pipeline.addLast(new MpegTsDecoder(analyzer), new TsHealthHandler(analyzer)));
```

Handlers receive each packet's payload as a `ByteBuf`, in sequence order:
normally seven 188-byte TS packets. Whoever consumes a buffer releases it.

Add handlers in the initializer, as above. It runs on the receiver's event loop
before the first packet is read, so the handlers see the stream from its start.
A handler added to `pipeline()` later, from another thread, is added
asynchronously by Netty, and packets read in the meantime pass it by. For a
callback instead of a handler:

```java
receiver.onData(payload -> {
    forward(payload);   // takes ownership
});
```

`onData` adds its handler on the event loop and returns once it is in place, so
every packet delivered after it returns reaches the callback.

Multicast, including source-specific:

```java
RtpReceiverConfig.multicast(InetAddress.getByName("239.1.1.1"), 5000)
        .withInterface(NetworkInterface.getByName("eth0"))
        .withSource(InetAddress.getByName("10.0.0.5"));
```

### What the receiver does

- **Validates** each datagram (RFC 3550 A.1): version 2, not an RTCP report on
  the wrong port, CSRC, extension and padding lengths that fit.
- **Locks to one source.** The first SSRC heard is the source. Another SSRC is
  counted and dropped while it is active. Once it has been silent for 500 ms or
  has sent RTCP BYE, the next SSRC takes over, so an encoder restart is followed
  without intervention.
- **Extends sequence numbers** past the 16-bit wrap and resynchronises when a
  source restarts its numbering (RFC 3550 A.1).
- **Reorders.** A packet that arrives in order is delivered at once. Behind a
  gap, packets wait until the oldest of them has waited the configured
  `latency` (120 ms by default). Then the gap is declared lost, reported to
  `RtpReceiverListener.onLoss`, and delivery continues.
- **Measures** loss and interarrival jitter as RFC 3550 A.3 and A.8 define them.
- **Reports** to the sender: a receiver report with SDES CNAME every 5 s
  (randomised by ±50%, §6.3.1) to the address its sender reports come from, and
  BYE on close. Sender reports are surfaced to listeners.

### Forward error correction

```java
RtpReceiverConfig.unicast(5000).withFec(true).withLatency(Duration.ofMillis(400));
```

With FEC on, the receiver also listens on P+2 for column FEC and P+4 for row
FEC (SMPTE 2022-1, the Pro-MPEG CoP #3 layout every common sender uses). It
needs no matrix settings: each FEC packet says what it protects, and the L×D
matrix in use is reported in `stats()`.

A missing packet is rebuilt as soon as one row or column it belongs to has
nothing else missing, and each recovery is re-checked against the other
dimension. So a matrix survives a burst of up to L consecutive losses
(through the columns) together with scattered single losses (through the
rows). Against ffmpeg's FEC, a link dropping 7.7% of packets in that pattern
delivered every one of them.

**Latency has to cover the FEC.** Column FEC for a matrix arrives spread over
the following matrix, so a gap can only be repaired that long after it opened.
Set `latency` to about two matrices of packets. For a 5×5 matrix at 3 Mbps
(about 300 packets a second), that is roughly 170 ms, so 300-400 ms is
comfortable. A recovery that arrives after delivery has given up is counted in
`packetsRecoveredLate`, and the first one logs a warning saying so.

### Configuration

| Setting | Default | |
|---|---|---|
| `latency` | 120 ms | How long a gap is waited on. Every packet behind a loss pays up to this much delay. |
| `rtcp` | on | RTCP on base port + 1. |
| `fec` | off | SMPTE 2022-1 column FEC on base port + 2, row FEC on + 4. |
| `receiveBufferBytes` | 4 MiB | The socket buffer to ask the OS for. Contribution streams arrive in bursts that overrun small defaults. |
| `networkInterface` | first up, multicast-capable, non-loopback | Multicast only. |
| `sourceFilter` | none | Multicast only: SSM source. |

Base port 0 picks a free port P for which the RTCP and FEC ports above it are
free too.

### Statistics

`receiver.stats()` returns a `ReceiverStats` snapshot, safe from any thread and
after close. Two loss figures, because they answer different questions:

- `networkLost` is what the network lost: RFC 3550's cumulative figure, the one
  receiver reports carry.
- `packetsLost` is what the application never got: packets given up on at
  delivery. With FEC, the first can be positive while the second stays zero.

With FEC: `packetsRecovered` counts packets rebuilt and never received.
FEC can overtake the media it protects, and a packet rebuilt that way and then
received is not counted. There is also `packetsRecoveredLate` (see above), and
the matrix in use (`fecColumns`, `fecRows`).

Also: delivered, duplicate, late, invalid and foreign-SSRC packet counts,
jitter in microseconds, source changes, and sender reports received.

## Sending

```java
RtpSender sender = RtpSender.connect(RtpSenderConfig.to(new InetSocketAddress("10.0.0.9", 5000))
        .withFec(5, 5));
sender.write(tsBytes);   // any amount, from any thread; takes ownership
```

`write` packs transport stream bytes into RTP packets of seven 188-byte TS
packets (1316 bytes, the most that fits a 1500-byte MTU). A packet goes out as
soon as it is full. `flush()` sends the whole TS packets waiting as a shorter
one, and keeps an incomplete TS packet for the next write, since RFC 2250 allows
only whole TS packets in a payload. For the same reason, `close()` discards an
incomplete TS packet at the very end. Sequence numbers start at
random, and timestamps are the 90 kHz send time, as RFC 2250 specifies.

**Pacing is the caller's.** A live source (a capture card, a Roast connection, a
Press receiver) produces a live-paced stream. A file written as fast as it can
be read goes out that fast.

The sender sends from a local port pair Q/Q+1 and sends to the destination's
P (media), P+1 (sender reports, every 5 s ±50%, and BYE on close), and with FEC
P+2 (column) and P+4 (row). Column FEC packets go out spread over the next
matrix, as ffmpeg sends them, not in a burst that a bursty link would lose
together. Receiver reports arriving at Q+1 update `stats()` with the
receiver's loss and jitter, and the round-trip time (RFC 3550 §6.4.1).

| Setting | Default | |
|---|---|---|
| `withFec(L, D)` | none | SMPTE 2022-1 limits: L 1-20, D 4-20, L×D at most 100. |
| `tsPacketsPerDatagram` | 7 | 1 to 7. |
| `payloadType` | 33 | MP2T (RFC 3551). |
| `withTtl`, `withInterface` | system defaults | Multicast destinations. |
| `rtcp` | on | |

## Composing with other transports

Press is one transport leg, as Roast is. Topology, meaning which inputs go to which
outputs, fan-out to several protocols, and failover between sources, belongs to
BrewStream, which composes the legs. Press provides the hops: a receiver's
pipeline delivers in-order payloads, and anything that writes them on works,
including an `RtpSender` or a Roast `SrtConnection`:

```java
RtpSender out = RtpSender.connect(RtpSenderConfig.to(destination));
RtpReceiver.bind(RtpReceiverConfig.unicast(5000), pipeline -> pipeline.addLast(
        new SimpleChannelInboundHandler<ByteBuf>(false) {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, ByteBuf payload) {
                out.write(payload);   // takes ownership
            }
        }));
```

## Threading and resources

Each receiver's or sender's sockets share one event loop, and its state lives
there, unsynchronised. Listeners run on that loop, so they must return quickly.
By default each has its own single-thread event loop group, shut down when it
closes. To run many, or to share with Roast, lend a group:

```java
PressTransport transport = PressTransport.shared(group, NioDatagramChannel.class);
RtpReceiver.bind(config, transport);
RtpSender.connect(senderConfig, transport);
```

`close()` sends what is pending, says BYE, and returns once the ports can be
bound again. Called on the receiver's or sender's own event loop (from a
listener, say), it cannot wait for that without blocking the loop it would wait
on, so the sockets close as soon as the loop is free and `close()` returns at
once.

With FEC, packets on the FEC ports are only accepted from the media source's
address (FEC carries no usable SSRC). Rebuilt packets go through the same header
checks as received ones, and FEC packets waiting to become useful are
deduplicated and capped, so corrupt or hostile FEC cannot grow memory or inject
malformed packets.

## Interoperability

Tested against ffmpeg (`./gradlew interopTest`, skipped when ffmpeg is absent):

| Direction | Peer | Checked |
|---|---|---|
| ffmpeg → Press | `ffmpeg -f rtp_mpegts` | no loss, zero TS continuity errors (Grind), every frame decodes, ffmpeg's RTCP SRs understood |
| ffmpeg → lossy link → Press | `-fec prompeg=l=5:d=5`, 7.7% of media dropped | every dropped packet recovered, zero continuity errors, every frame decodes. Without FEC the same link causes continuity errors |
| Press encoder vs ffmpeg | the same media through both | every FEC packet identical in every recovery field and payload byte |
| Press → ffmpeg | `ffmpeg -i rtp://...` (PT 33, no SDP) | decodes with no errors and no continuity errors; 124 of 150 frames recorded, against 123 when ffmpeg's own sender feeds it the same way (ffmpeg drops what it uses for probing) |

ffmpeg's RTP input sent no receiver reports to the sender in these runs, so the
sender's round-trip time is verified Press-to-Press only.

## Roadmap

1. ~~Receive path: RTP, RTCP, reordering, statistics~~
2. ~~SMPTE 2022-1 FEC recovery, column and row~~
3. ~~Sender with optional FEC~~
4. Next: RIST simple profile (RTP plus NACK retransmission), and raw UDP
   transport streams without RTP, which many contribution links still use.

Not planned: RTP payload formats for elementary streams (H.264 RFC 6184 and so
on), SRTP, and SMPTE ST 2110. Press carries transport streams.

## References

RFC 3550 (RTP and RTCP), RFC 3551 (payload type 33), RFC 2250 (MPEG-TS over
RTP), RFC 2733 (the FEC scheme SMPTE 2022-1 builds on). Where Press departs
from an RFC, the javadoc on the class says so and why. `SequenceTracker` is the
place to start.

## Licence

Apache 2.0. See `LICENSE` and `NOTICE`.
