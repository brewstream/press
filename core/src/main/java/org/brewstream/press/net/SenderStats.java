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
 * A snapshot of one sender's counters, and what the receiver last reported,
 * from {@link RtpSender#stats()}.
 *
 * @param ssrc            the sender's SSRC
 * @param packetsSent     media packets sent
 * @param bytesSent       payload bytes sent, as RTCP's octet count reckons them
 * @param fecPacketsSent  FEC packets sent, both directions
 * @param receiverReports RTCP receiver reports received about this stream
 * @param receiver        where the last one came from, or {@code null}
 * @param fractionLost    the receiver's loss over its last report interval, 0.0 to 1.0
 * @param cumulativeLost  the receiver's RFC 3550 cumulative loss
 * @param jitterMicros    the receiver's interarrival jitter
 * @param rttMicros       round-trip time from the last report (RFC 3550 §6.4.1), or -1 until
 *                        a report echoes one of this sender's reports
 */
public record SenderStats(
        long ssrc,
        long packetsSent,
        long bytesSent,
        long fecPacketsSent,
        long receiverReports,
        InetSocketAddress receiver,
        double fractionLost,
        long cumulativeLost,
        long jitterMicros,
        long rttMicros) {
}