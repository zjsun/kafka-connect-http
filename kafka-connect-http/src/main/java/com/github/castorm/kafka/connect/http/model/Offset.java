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

import com.google.common.collect.Maps;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public class Offset implements Map<String, Object> {

    private static final String KEY_KEY = "key";

    private static final String TIMESTAMP_KEY = "timestamp";

    private static final String KEY_PS = "ps"; // page size
    private static final String KEY_PI = "pi"; // page index
    private static final String KEY_PT = "pt"; // page total/count
    private static final String KEY_PP = "pp"; // first page index

    public static final String KEY_SNAPSHOTING = "SNAPSHOTING";
    public static final String KEY_PAGINATING = "PAGINATING";

    private final Map<String, Object> properties = Maps.newConcurrentMap();

    private Offset(Map<String, ?> properties) {
        this.properties.putAll(properties);
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
    public Offset updatePage(Map<String, ?> update, boolean pi, boolean ps, boolean pt) {
        if (ps && update.containsKey(KEY_PS)) this.put(KEY_PS, update.get(KEY_PS));
        if (pi && update.containsKey(KEY_PI)) this.put(KEY_PI, update.get(KEY_PI));
        if (pt && update.containsKey(KEY_PT)) this.put(KEY_PT, update.get(KEY_PT));
        if (update.containsKey(KEY_PP)) this.put(KEY_PP, update.get(KEY_PP));
        return this;
    }

    public Offset updatePi(int pi) {
        this.put(KEY_PI, pi);
        return this;
    }

    public Offset update(Map<String, ?> update) {
        this.putAll(update);
        return this;
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

    public boolean isSnapshoting() {
        return ((Boolean) properties.get(KEY_SNAPSHOTING)).booleanValue();
    }

    public void setSnapshoting(boolean snapshoting) {
        properties.put(KEY_SNAPSHOTING, snapshoting);
    }

    public boolean isPaginating() {
        return ((Boolean) properties.get(KEY_PAGINATING)).booleanValue();
    }

    public void setPaginating(boolean paginating) {
        properties.put(KEY_PAGINATING, paginating);
    }

    public int size() {
        return properties.size();
    }

    public boolean isEmpty() {
        return properties.isEmpty();
    }

    public boolean containsKey(Object key) {
        return properties.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return properties.containsValue(value);
    }

    public Object get(Object key) {
        return properties.get(key);
    }

    public Object put(String key, Object value) {
        return properties.put(key, value);
    }

    public Object remove(Object key) {
        return properties.remove(key);
    }

    public void putAll(@NotNull Map<? extends String, ?> m) {
        properties.putAll(m);
    }

    public void clear() {
        properties.clear();
    }

    public Set<String> keySet() {
        return properties.keySet();
    }

    public Collection<Object> values() {
        return properties.values();
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return properties.entrySet();
    }

    public Object getOrDefault(Object key, Object defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    public void forEach(BiConsumer<? super String, ? super Object> action) {
        properties.forEach(action);
    }

    public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
        properties.replaceAll(function);
    }

    public Object putIfAbsent(String key, Object value) {
        return properties.putIfAbsent(key, value);
    }

    public boolean remove(Object key, Object value) {
        return properties.remove(key, value);
    }

    public boolean replace(String key, Object oldValue, Object newValue) {
        return properties.replace(key, oldValue, newValue);
    }

    public Object replace(String key, Object value) {
        return properties.replace(key, value);
    }

    public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
        return properties.computeIfAbsent(key, mappingFunction);
    }

    public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        return properties.computeIfPresent(key, remappingFunction);
    }

    public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        return properties.compute(key, remappingFunction);
    }

    public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        return properties.merge(key, value, remappingFunction);
    }

    @Override
    public boolean equals(Object o) {
        return properties.equals(o);
    }

    @Override
    public int hashCode() {
        return properties.hashCode();
    }

    @Override
    public String toString() {
        return properties.toString();
    }
}
