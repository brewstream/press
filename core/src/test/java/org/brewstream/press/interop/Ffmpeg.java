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

package org.brewstream.press.interop;

import org.brewstream.grind.TsAnalyzer;
import org.brewstream.grind.TsPacket;
import org.brewstream.grind.TsStreamStats;
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Locating ffmpeg, making a test stream with it, and checking a received one. */
final class Ffmpeg {

    /** Seconds of test stream. Long enough for a few RTCP reports and many FEC matrices. */
    static final int FIXTURE_SECONDS = 6;
    static final int FIXTURE_FRAMES = FIXTURE_SECONDS * 25;

    private Ffmpeg() {
    }

    /** The ffmpeg binary: {@code $FFMPEG}, else {@code ffmpeg} on the PATH. Skips the test if absent. */
    static String binary() {
        String binary = System.getenv().getOrDefault("FFMPEG", "ffmpeg");
        Assumptions.assumeTrue(runs(binary, "-version"), "ffmpeg not found - set FFMPEG or put it on the PATH");
        return binary;
    }

    static String ffprobe() {
        String binary = System.getenv().getOrDefault("FFPROBE", "ffprobe");
        Assumptions.assumeTrue(runs(binary, "-version"), "ffprobe not found - set FFPROBE or put it on the PATH");
        return binary;
    }

    /**
     * A transport stream of MPEG-2 video and MP2 audio, encoders every ffmpeg
     * build carries, at about 3 Mbps. Made once per build directory and reused.
     */
    static Path fixture() throws IOException, InterruptedException {
        Path fixture = Path.of("build", "interop", "fixture.ts");
        if (Files.exists(fixture)) {
            return fixture;
        }
        Files.createDirectories(fixture.getParent());
        Path partial = fixture.resolveSibling("fixture.ts.partial");
        run(List.of(binary(), "-y", "-v", "error",
                "-f", "lavfi", "-i", "testsrc=size=640x360:rate=25,noise=alls=40:allf=t",
                "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
                "-t", String.valueOf(FIXTURE_SECONDS),
                // Noise keeps a test pattern from compressing to nothing: about 3 Mbps,
                // 300 packets a second, so an FEC matrix lasts tens of milliseconds as
                // it would on a real contribution feed.
                "-c:v", "mpeg2video", "-b:v", "3000k", "-minrate", "3000k", "-maxrate", "3000k",
                "-bufsize", "1500k", "-g", "25",
                "-c:a", "mp2", "-b:a", "128k",
                "-f", "mpegts", partial.toString()), 60);
        Files.move(partial, fixture);
        return fixture;
    }

    /** Starts ffmpeg with stderr kept in a file, which the returned handle can show on failure. */
    static Running start(List<String> arguments) throws IOException {
        Path log = Files.createTempFile("press-ffmpeg-", ".log");
        log.toFile().deleteOnExit();
        Process process = new ProcessBuilder(arguments)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(log.toFile())
                .start();
        return new Running(process, log);
    }

    record Running(Process process, Path log) implements AutoCloseable {
        boolean waitFor(long seconds) throws InterruptedException {
            return process.waitFor(seconds, TimeUnit.SECONDS);
        }

        String logText() throws IOException {
            return Files.readString(log);
        }

        @Override
        public void close() {
            process.destroyForcibly();
        }
    }

    /** Video frames ffprobe can decode from a file. */
    static int decodableVideoFrames(Path file) throws IOException, InterruptedException {
        Process probe = new ProcessBuilder(ffprobe(), "-v", "error", "-select_streams", "v:0", "-count_frames",
                "-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", file.toString())
                .redirectErrorStream(true)
                .start();
        String out = new String(probe.getInputStream().readAllBytes()).trim();
        probe.waitFor(60, TimeUnit.SECONDS);
        // Some ffprobe versions print a trailing separator after the field ("150,").
        String last = out.lines().reduce((a, b) -> b).orElse("0").replaceAll("[^0-9]", "");
        return last.isEmpty() ? 0 : Integer.parseInt(last);
    }

    /** Runs Grind over a transport stream held in memory. */
    static TsStreamStats analyse(byte[] ts) {
        TsAnalyzer analyzer = new TsAnalyzer();
        for (int at = 0; at + TsPacket.LENGTH <= ts.length; at += TsPacket.LENGTH) {
            analyzer.consume(TsPacket.parse(ts, at));
        }
        return analyzer.stats();
    }

    private static boolean runs(String binary, String argument) {
        try {
            Process process = new ProcessBuilder(binary, argument).redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            return process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void run(List<String> command, int timeoutSeconds) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(new ArrayList<>(command)).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IOException("ffmpeg failed: " + command + "\n" + output);
        }
    }
}