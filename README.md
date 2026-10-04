# Press

Pure-Java RTP transport for MPEG-TS. Part of **BrewStream**: Press is the RTP
leg, as [Roast](https://github.com/brewstream/roast) is the SRT leg.

Press carries a transport stream over RTP the way broadcast contribution links
do: RFC 3550 RTP and RTCP, the RFC 2250 / SMPTE 2022-2 MPEG-TS payload, SMPTE
2022-1 forward error correction, unicast or multicast. A receiver puts packets
back in order, rebuilds lost ones from FEC, decides when a missing one is gone
for good, keeps the RFC 3550 statistics, and reports them to the sender over
RTCP. Its data path is a Netty pipeline, so a stream received by Press is
inspected with [Grind](https://github.com/brewstream/grind) exactly as an SRT
stream received by Roast is.

**Status:** in development, not yet released. The receive path, including
SMPTE 2022-1 FEC, is done. The sender is next; see [Roadmap](#roadmap).

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

### Threading and resources

A receiver's sockets share one event loop and its state lives there,
unsynchronised. Listeners run on that loop, so they must return quickly. By
default each receiver has its own single-thread event loop group, shut down when
it closes. To run many receivers, or to share with Roast, lend a group:

```java
PressTransport transport = PressTransport.shared(group, NioDatagramChannel.class);
RtpReceiver.bind(config, transport);
```

`close()` delivers whatever is waiting behind a gap, sends BYE, and returns once
the ports can be bound again.

## Interoperability

Tested against ffmpeg (`./gradlew interopTest`, skipped when ffmpeg is absent):

| Direction | Peer | Checked |
|---|---|---|
| ffmpeg → Press | `ffmpeg -f rtp_mpegts` | no loss, zero TS continuity errors (Grind), every frame decodes, ffmpeg's RTCP SRs understood |
| ffmpeg → lossy link → Press | `-fec prompeg=l=5:d=5`, 7.7% of media dropped | every dropped packet recovered, zero continuity errors, every frame decodes. Without FEC the same link causes continuity errors |
| Press encoder vs ffmpeg | the same media through both | every FEC packet identical in every recovery field and payload byte |

## Roadmap

1. ~~Receive path: RTP, RTCP, reordering, statistics~~
2. ~~SMPTE 2022-1 FEC recovery, column and row~~
3. Sender with optional FEC, and fan-out to many destinations
4. Later: RIST simple profile (RTP plus NACK retransmission)

Not planned: RTP payload formats for elementary streams (H.264 RFC 6184 and so
on), SRTP, and SMPTE ST 2110. Press carries transport streams.

## References

RFC 3550 (RTP and RTCP), RFC 3551 (payload type 33), RFC 2250 (MPEG-TS over
RTP), RFC 2733 (the FEC scheme SMPTE 2022-1 builds on). Where Press departs
from an RFC, the javadoc on the class says so and why. `SequenceTracker` is the
place to start.

## Licence

Apache 2.0. See `LICENSE` and `NOTICE`.
