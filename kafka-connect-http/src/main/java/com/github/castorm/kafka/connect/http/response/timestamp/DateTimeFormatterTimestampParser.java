package com.github.castorm.kafka.connect.http.response.timestamp;

/*-
 * #%L
 * Kafka Connect HTTP Plugin
 * %%
 * Copyright (C) 2020 CastorM
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Function;

@RequiredArgsConstructor
public class DateTimeFormatterTimestampParser implements TimestampParser {

    private final Function<Map<String, ?>, DateTimeFormatterTimestampParserConfig> configFactory;

    private DateTimeFormatter timestampFormatter;

    public DateTimeFormatterTimestampParser() {
        this(DateTimeFormatterTimestampParserConfig::new);
    }

    @Override
    public void configure(Map<String, ?> settings) {
        timestampFormatter = configFactory.apply(settings).getRecordTimestampFormatter();
    }

    @Override
    public Instant parse(String timestamp) {
        return ZonedDateTime.parse(timestamp, timestampFormatter).toInstant(); // datav fix: Unable to obtain OffsetDateTime from TemporalAccessor
    }
}
