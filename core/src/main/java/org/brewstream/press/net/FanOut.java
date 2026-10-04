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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Copies every payload passing through a pipeline to any number of
 * destinations, which can be added and removed while the stream runs.
 *
 * <pre>{@code
 * FanOut fanOut = new FanOut();
 * RtpReceiver.bind(RtpReceiverConfig.unicast(5000), pipeline -> pipeline.addLast(fanOut));
 *
 * fanOut.add("studio-b", RtpSender.connect(RtpSenderConfig.to(studioB))::write);
 * fanOut.add("archive", srtConnection::write);     // a Roast connection works the same way
 * fanOut.remove("studio-b");
 * }</pre>
 *
 * <p>A destination is any {@code Consumer<ByteBuf>}: {@link RtpSender#write},
 * a Roast {@code SrtConnection.write}, or anything else that takes ownership of
 * a buffer. Each destination gets its own reference to the same bytes (a
 * retained duplicate), so there is no copying. It must release what it is
 * given, and must not block: it is called on the receiving event loop, and
 * {@code RtpSender.write} and Roast's {@code write} both hand off and return.
 * One that throws is logged; the others still get the payload. It still owns
 * the copy it was given, and must release it even when it throws.
 *
 * <p>The handler works on any pipeline whose messages are payload
 * {@code ByteBuf}s, so it fits a Roast connection's pipeline as well as a Press
 * receiver's. Payloads are passed on down the pipeline afterwards, so handlers
 * after it, such as an analyzer, still see the stream. Sharable, but one
 * instance is normally one stream.
 */
@ChannelHandler.Sharable
public final class FanOut extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = Logger.getLogger(FanOut.class.getName());

    private final Map<String, Consumer<ByteBuf>> destinations = new ConcurrentHashMap<>();

    /**
     * Adds a destination, or replaces the one with the same name. It receives
     * payloads from the next one passing through.
     */
    public void add(String name, Consumer<ByteBuf> destination) {
        destinations.put(name, destination);
    }

    /** Removes a destination. A payload already being handed out may still reach it. */
    public boolean remove(String name) {
        return destinations.remove(name) != null;
    }

    /** The names of the current destinations. */
    public Set<String> destinations() {
        return Set.copyOf(destinations.keySet());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf payload) {
            for (Map.Entry<String, Consumer<ByteBuf>> entry : destinations.entrySet()) {
                try {
                    entry.getValue().accept(payload.retainedDuplicate());
                } catch (RuntimeException e) {
                    // The destination owns its copy even when it throws: a duplicate shares
                    // the original's reference count, so whether it released cannot be seen.
                    LOG.log(Level.WARNING, "fan-out destination " + entry.getKey() + " threw", e);
                }
            }
        }
        ctx.fireChannelRead(msg);
    }
}