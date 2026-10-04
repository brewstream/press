# Press

Pure-Java RTP transport for MPEG-TS. Part of **BrewStream**: Press is the RTP
leg, as [Roast](https://github.com/brewstream/roast) is the SRT leg.

Press carries a transport stream over RTP the way broadcast contribution links
do: RFC 3550 RTP and RTCP, the RFC 2250 / SMPTE 2022-2 MPEG-TS payload, unicast
or multicast. A receiver puts packets back in order, decides when a missing one
is lost, keeps the RFC 3550 statistics, and reports them to the sender over
RTCP. Its data path is a Netty pipeline, so a stream received by Press is
inspected with [Grind](https://github.com/brewstream/grind) exactly as an SRT
stream received by Roast is.

**Status:** in development, not yet released. The receive path is done. SMPTE
2022-1 FEC and the sender are next; see [Roadmap](#roadmap).

## Requirements

Java 21 or newer. Netty 4.2 is the only runtime dependency.

## Receiving

```java
RtpReceiver receiver = RtpReceiver.bind(RtpReceiverConfig.unicast(5000));

TsAnalyzer analyzer = new TsAnalyzer();
receiver.pipeline().addLast(new MpegTsDecoder(analyzer), new TsHealthHandler(analyzer));
```

Handlers added to `pipeline()` receive each packet's payload as a `ByteBuf`,
in sequence order: normally seven 188-byte TS packets. Whoever consumes a buffer
releases it. For a callback instead of a handler:

```java
receiver.onData(payload -> {
    forward(payload);   // takes ownership
});
```

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

### Configuration

| Setting | Default | |
|---|---|---|
| `latency` | 120 ms | How long a gap is waited on. Every packet behind a loss pays up to this much delay. |
| `rtcp` | on | RTCP on base port + 1. |
| `receiveBufferBytes` | 4 MiB | The socket buffer to ask the OS for. Contribution streams arrive in bursts that overrun small defaults. |
| `networkInterface` | first up, multicast-capable, non-loopback | Multicast only. |
| `sourceFilter` | none | Multicast only: SSM source. |

Base port 0 picks a free port P for which P+1 is free too, so an ephemeral
receiver still gets RTCP.

### Statistics

`receiver.stats()` returns a `ReceiverStats` snapshot, safe from any thread and
after close. Two loss figures, because they answer different questions:

- `networkLost` is what the network lost: RFC 3550's cumulative figure, the one
  receiver reports carry.
- `packetsLost` is what the application never got: packets given up on at
  delivery. With FEC, the first can be positive while the second stays zero.

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

## Roadmap

1. ~~Receive path: RTP, RTCP, reordering, statistics~~ (this release)
2. SMPTE 2022-1 FEC recovery, column and row
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
