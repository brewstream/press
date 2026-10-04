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

import java.net.InetSocketAddress;

/**
 * A snapshot of one receiver's counters, from {@link RtpReceiver#stats()}.
 *
 * <p>Two loss figures, because they answer different questions:
 * {@code networkLost} is what the network lost, RFC 3550's cumulative figure
 * that also goes into receiver reports. {@code packetsLost} is what the
 * application never got: packets declared lost at delivery, after reordering
 * (and, with FEC, recovery) had their chance. On a healthy FEC-protected stream
 * the first is positive and the second is zero.
 *
 * @param ssrc             the source being received, or -1 before the first packet
 * @param source           where its packets come from, or {@code null}
 * @param packetsReceived  valid packets from the source, duplicates and late ones included
 * @param packetsDelivered packets handed to the pipeline, in order
 * @param packetsLost      packets given up on at delivery; the pipeline saw a gap here
 * @param packetsDuplicate packets that had already been received
 * @param packetsLate      packets that arrived after they had been given up on
 * @param packetsInvalid   datagrams that failed RTP header checks, or whose sequence number
 *                         was too far out of range to place
 * @param packetsForeign   packets from another SSRC while this source was active
 * @param bytesDelivered   payload bytes handed to the pipeline
 * @param networkLost      RFC 3550 cumulative loss: expected minus received. Duplicates can
 *                         make it negative
 * @param jitterMicros     RFC 3550 interarrival jitter
 * @param sourceChanges    times the receiver moved to a new SSRC, or resynchronised after the
 *                         same SSRC restarted its sequence numbers
 * @param senderReports    RTCP sender reports received from the source
 */
public record ReceiverStats(
        long ssrc,
        InetSocketAddress source,
        long packetsReceived,
        long packetsDelivered,
        long packetsLost,
        long packetsDuplicate,
        long packetsLate,
        long packetsInvalid,
        long packetsForeign,
        long bytesDelivered,
        long networkLost,
        long jitterMicros,
        long sourceChanges,
        long senderReports) {
}