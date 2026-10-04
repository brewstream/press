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

import org.brewstream.press.packet.RtcpPacket;

import java.net.InetSocketAddress;

/**
 * Events from a {@link RtpReceiver}. Every method has an empty default, so
 * implement only what you need. Called on the receiver's event loop: return
 * quickly, and hand anything slow to another thread. An exception thrown here is
 * logged and does not affect the receiver.
 */
public interface RtpReceiverListener {

    /**
     * The receiver started on a source, moved to a new one, or resynchronised
     * because the same SSRC restarted its sequence numbers.
     *
     * @param previousSsrc the SSRC before, or -1 for the first source
     */
    default void onSourceChanged(RtpReceiver receiver, long previousSsrc, long ssrc, InetSocketAddress source) {
    }

    /**
     * {@code count} packets starting at {@code firstExtendedSeq} will never be
     * delivered. The pipeline sees the bytes before and after with nothing in
     * between, which a transport-stream parser will notice as continuity errors.
     */
    default void onLoss(RtpReceiver receiver, long firstExtendedSeq, int count) {
    }

    /**
     * FEC rebuilt a missing packet in time for it to be delivered in its place.
     * FEC arrives on other ports and can overtake the media it protects, so the
     * original may still turn up afterwards; it is then discarded, and taken out
     * of {@link ReceiverStats#packetsRecovered()}. Recoveries that come too late
     * are only counted, in {@link ReceiverStats#packetsRecoveredLate()}.
     */
    default void onRecovered(RtpReceiver receiver, long extendedSeq) {
    }

    /** An RTCP sender report arrived from the source. */
    default void onSenderReport(RtpReceiver receiver, RtcpPacket.SenderReport report) {
    }

    /** The source sent RTCP BYE: it says it has stopped. */
    default void onGoodbye(RtpReceiver receiver, long ssrc) {
    }
}