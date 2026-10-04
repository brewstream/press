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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NtpTimeTest {

    @Test
    void theUnixEpochIsNtpSecond2208988800() {
        assertThat(NtpTime.fromEpochMicros(0)).isEqualTo(2_208_988_800L << 32);
    }

    @Test
    void halfASecondIsHalfTheFractionRange() {
        long ntp = NtpTime.fromEpochMicros(500_000);

        assertThat(ntp >>> 32).isEqualTo(2_208_988_800L);
        assertThat(ntp & 0xFFFF_FFFFL).isEqualTo(0x8000_0000L);
    }

    @Test
    void compactUnitsAreSixtyFiveThousandFiveHundredThirtySixthsOfASecond() {
        assertThat(NtpTime.nanosToCompact(1_000_000_000L)).isEqualTo(65_536);
        assertThat(NtpTime.compactToMicros(32_768)).isEqualTo(500_000);
        // 1.5 s after the Unix epoch is NTP second 2208988801 = 0x83AA7E81, plus half a second.
        assertThat(NtpTime.compact(NtpTime.fromEpochMicros(1_500_000))).isEqualTo((0x7E81L << 16) + 0x8000);
    }
}