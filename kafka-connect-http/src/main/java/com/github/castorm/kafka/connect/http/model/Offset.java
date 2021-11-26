package com.github.castorm.kafka.connect.http.model;

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

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.*;

import static java.util.Optional.ofNullable;

@ToString
@EqualsAndHashCode
public class Offset {

    private static final String KEY_KEY = "key";

    private static final String TIMESTAMP_KEY = "timestamp";

    private static final String KEY_PS = "ps"; // page size
    private static final String KEY_PI = "pi"; // page index
    private static final String KEY_PT = "pt"; // page total/count
    private static final String KEY_PP = "pp"; // first page index

    private final Map<String, Object> properties;

    private Offset(Map<String, ?> properties) {
        this.properties = (Map<String, Object>) properties;
    }

    public static Offset of(Map<String, ?> properties) {
        return new Offset(properties);
    }

    public static Offset of(Map<String, ?> properties, String key) {
        Map<String, Object> props = new HashMap<>(properties);
        props.put(KEY_KEY, key);
        return new Offset(props);
    }

    public static Offset of(Map<String, ?> properties, String key, Instant timestamp) {
        Map<String, Object> props = new HashMap<>(properties);
        props.put(KEY_KEY, key);
        props.put(TIMESTAMP_KEY, timestamp.toString());
        return new Offset(props);
    }

    // 更新分页信息
    public static Offset updatePage(Map<String, ?> origin, Map<String, ?> update, boolean pi, boolean ps, boolean pt) {
        Map<String, Object> props = new HashMap<>(origin);
        if (ps && update.containsKey(KEY_PS)) props.put(KEY_PS, update.get(KEY_PS));
        if (pi && update.containsKey(KEY_PI)) props.put(KEY_PI, update.get(KEY_PI));
        if (pt && update.containsKey(KEY_PT)) props.put(KEY_PT, update.get(KEY_PT));
        if (update.containsKey(KEY_PP)) props.put(KEY_PP, update.get(KEY_PP));
        return new Offset(props);
    }

    public static Offset updatePi(Map<String, ?> origin, int pi) {
        return updatePage(origin, Collections.singletonMap(KEY_PI, String.valueOf(pi)), true, false, false);
    }

    public Map<String, ?> toMap() {
        return properties;
    }

    public Optional<String> getKey() {
        return ofNullable((String) properties.get(KEY_KEY));
    }

    public Optional<Instant> getTimestamp() {
        return ofNullable((String) properties.get(TIMESTAMP_KEY)).map(Instant::parse);
    }

    public Optional<Pageable> getPageable() {
        Pageable request = null;
        if (properties.containsKey(KEY_PS) && properties.containsKey(KEY_PI)) {
            int ps = Integer.parseInt((String) properties.get(KEY_PS));
            int pi = Integer.parseInt((String) properties.get(KEY_PI));
            request = PageRequest.of(pi, ps);
        }
        return Optional.ofNullable(request);
    }

    public Optional<Page> getPage() {
        Page page = null;
        Pageable request = getPageable().orElse(null);
        if (request != null) {
            if (properties.containsKey(KEY_PT)) {
                page = new PageImpl(Arrays.asList(new Object[request.getPageSize()]), request, Long.parseLong((String) properties.get(KEY_PT)));
            }
        }
        return Optional.ofNullable(page);
    }

    public int getPp() {
        return Integer.parseInt((String) properties.getOrDefault(KEY_PP, "0"));
    }
}
