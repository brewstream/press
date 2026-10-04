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

package org.brewstream.press.packet;

/**
 * 64-bit NTP timestamps (RFC 3550 §4): seconds since 1900 in the high 32 bits,
 * the fraction of a second in the low 32. RTCP only uses them to compare
 * moments, so precision past a microsecond does not matter.
 */
public final class NtpTime {

    /** Seconds from 1900-01-01 (NTP's epoch) to 1970-01-01 (Java's). */
    private static final long EPOCH_OFFSET_SECONDS = 2_208_988_800L;

    private NtpTime() {
    }

    /** The NTP timestamp for a wall-clock instant in microseconds since 1970. */
    public static long fromEpochMicros(long epochMicros) {
        long seconds = Math.floorDiv(epochMicros, 1_000_000L) + EPOCH_OFFSET_SECONDS;
        long micros = Math.floorMod(epochMicros, 1_000_000L);
        long fraction = (micros << 32) / 1_000_000L;
        return seconds << 32 | fraction;
    }

    /** Now, from the system clock. */
    public static long now() {
        java.time.Instant now = java.time.Instant.now();
        return fromEpochMicros(now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000);
    }

    /** The middle 32 bits, the compact form used for LSR, DLSR and round-trip time. */
    public static long compact(long ntpTimestamp) {
        return ntpTimestamp >>> 16 & 0xFFFF_FFFFL;
    }

    /** Converts nanoseconds to compact NTP units of 1/65536 s. */
    public static long nanosToCompact(long nanos) {
        return nanos / 1_000L * 65_536L / 1_000_000L; // via micros, so it cannot overflow
    }

    /** Converts compact NTP units of 1/65536 s to microseconds. */
    public static long compactToMicros(long compact) {
        return compact * 1_000_000L / 65_536L;
    }
}