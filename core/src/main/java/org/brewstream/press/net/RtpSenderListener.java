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
 * Events from a {@link RtpSender}, called on its event loop. Return quickly; an
 * exception thrown here is logged and does not affect the sender.
 */
public interface RtpSenderListener {

    /**
     * A receiver reported on this stream.
     *
     * @param rttMicros round-trip time computed from the report, or -1 if it does not
     *                  echo one of this sender's reports yet
     */
    default void onReceiverReport(RtpSender sender, InetSocketAddress from, RtcpPacket.ReportBlock report,
            long rttMicros) {
    }

    /** A receiver sent RTCP BYE. */
    default void onGoodbye(RtpSender sender, InetSocketAddress from, long ssrc) {
    }
}