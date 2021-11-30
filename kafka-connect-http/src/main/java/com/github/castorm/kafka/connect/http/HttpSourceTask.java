package com.github.castorm.kafka.connect.http;

/*-
 * #%L
 * kafka-connect-http
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

import com.github.castorm.kafka.connect.http.ack.ConfirmationWindow;
import com.github.castorm.kafka.connect.http.auth.spi.HttpAuthenticator;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordFilterFactory;
import com.github.castorm.kafka.connect.http.record.spi.SourceRecordSorter;
import com.github.castorm.kafka.connect.http.request.spi.HttpRequestFactory;
import com.github.castorm.kafka.connect.http.response.spi.HttpResponseParser;
import com.github.castorm.kafka.connect.timer.TimerThrottler;
import com.github.castorm.kafka.connect.util.ConfigUtils;
import com.github.castorm.kafka.connect.util.ExitUtils;
import edu.emory.mathcs.backport.java.util.Collections;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.github.castorm.kafka.connect.common.VersionUtils.getVersion;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

@Slf4j
public class HttpSourceTask extends SourceTask {

    private final Function<Map<String, String>, HttpSourceConnectorConfig> configFactory;

    private TimerThrottler throttler;

    private HttpRequestFactory requestFactory;

    private HttpClient requestExecutor;

    private HttpResponseParser responseParser;

    private SourceRecordSorter recordSorter;

    private SourceRecordFilterFactory recordFilterFactory;

    private ConfirmationWindow<Map<String, ?>> confirmationWindow = new ConfirmationWindow<>(emptyList());

    private HttpAuthenticator authenticator;

    @Getter
    private Offset offset;

    private HttpSourceConnectorConfig config;
    private final AtomicBoolean snapshoting = new AtomicBoolean(true); // 正在初始快照（首次迭代）
    private final AtomicBoolean paginating = new AtomicBoolean(false); // 正在翻页执行标记

    HttpSourceTask(Function<Map<String, String>, HttpSourceConnectorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    public HttpSourceTask() {
        this(HttpSourceConnectorConfig::new);
    }

    @Override
    public void start(Map<String, String> settings) {
        config = configFactory.apply(settings);

        throttler = config.getThrottler();
        requestFactory = config.getRequestFactory();
        requestExecutor = config.getClient();
        responseParser = config.getResponseParser();
        recordSorter = config.getRecordSorter();
        recordFilterFactory = config.getRecordFilterFactory();
        offset = loadOffset(config.getInitialOffset());

        // pre auth
        authenticator = requestExecutor.getAuthenticator();
        if (authenticator != null){
            offset = authenticator.authenticate(requestExecutor, offset);
        }
    }

    private Offset loadOffset(Map<String, String> initialOffset) {
        Map<String, Object> restoredOffset = ofNullable(context.offsetStorageReader().offset(emptyMap())).orElseGet(Collections::emptyMap);
        return Offset.of(!restoredOffset.isEmpty() ? restoredOffset : initialOffset);
    }

    void checkIfTaskDone() {
        if (ConfigUtils.isDkeTaskMode(this.config)) {
            if (HttpSourceConnector.taskCount.decrementAndGet() <= 0) {
                // do something finally if needed
            }
            throw new ConnectException(ExitUtils.MSG_DONE);// force task stop
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // 1) 每次分页迭代开始需要重置翻页参数；2）快照阶段不使用间隔等待
        if (!paginating.get()) {
            if (snapshoting.get()) {
                Utils.sleep(2000); // 延迟开始快照
            } else {
                checkIfTaskDone();
                throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));
            }

            offset = Offset.updatePage(offset.toMap(), config.getInitialOffset(), true, true, false); // 新迭代开始时重置翻页
        }

        Pageable pageRequest = offset.getPageable().orElse(null);

        HttpRequest request = requestFactory.createRequest(offset);

        HttpResponse response = execute(request);

        List<SourceRecord> records = responseParser.parse(response);

        List<SourceRecord> unseenRecords = recordSorter.sort(records).stream()
                .filter(recordFilterFactory.create(offset))
                .collect(toList());

        log.info("Request for offset {} yields {}/{} new records", offset.toMap(), unseenRecords.size(), records.size());

        List<Map<String, ?>> recordOffsets = extractOffsets(unseenRecords);
        confirmationWindow = new ConfirmationWindow<>(recordOffsets);

        offset = recordOffsets.stream().findFirst().map(map -> Offset.updatePage(offset.toMap(), map, true, true, true)).orElse(offset);

        Page pageResult = offset.getPage().orElse(null);
        if (pageResult != null) {
            if (pageResult.hasNext()) { // 进入翻页过程，并置下一页
                paginating.set(true);
                int nextPi = pageResult.nextPageable().getPageNumber() + offset.getPp();
                if (nextPi == pageRequest.getPageNumber()) {
                    throw new ConnectException("请正确设置pp(首页序号)并清理Offset后重试");
                }
                offset = Offset.updatePi(offset.toMap(), nextPi);
            } else { // 退出翻页过程，退出快照过程
                paginating.set(false);
                snapshoting.set(false);
            }
        } else { // 没有翻页退出快照过程
            snapshoting.set(false);
        }

        return unseenRecords;
    }

    private HttpResponse execute(HttpRequest request) {
        try {
            return requestExecutor.execute(request);
        } catch (IOException e) {
            throw new RetriableException(e);
        }
    }

    private static List<Map<String, ?>> extractOffsets(List<SourceRecord> recordsToSend) {
        return recordsToSend.stream()
                .map(SourceRecord::sourceOffset)
                .collect(toList());
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        confirmationWindow.confirm(record.sourceOffset());
    }

    @Override
    public void commit() {
        Offset commited = confirmationWindow.getLowWatermarkOffset().map(Offset::of).orElse(offset);

        log.debug("Offset committed: {}", commited);
    }

    @Override
    public void stop() {
        // Nothing to do, no resources to release
    }

    @Override
    public String version() {
        return getVersion();
    }
}
