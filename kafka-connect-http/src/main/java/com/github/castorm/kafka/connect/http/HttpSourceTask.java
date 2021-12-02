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
import com.github.castorm.kafka.connect.util.ScriptUtils;
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

        // init offset
        offset = loadOffset(config.getInitialOffset());

        // pre auth
        authenticator = requestExecutor.getAuthenticator();
        if (authenticator != null) {
            offset = authenticator.authenticate(requestExecutor, offset);
        }

    }

    // 初始化offset
    private Offset loadOffset(Map<String, String> initialOffset) {
        Offset offset = Offset.of(initialOffset);
        offset.setSnapshoting(true);
        offset.setPaginating(false);

        Map<String, Object> restoredOffset = ofNullable(context.offsetStorageReader().offset(emptyMap())).orElseGet(Collections::emptyMap);
        offset.update(restoredOffset);

        offset = ScriptUtils.evalScript(config.getPollScriptInit(), offset);
        return offset;
    }

    void checkIfTaskDone() {
        if (ConfigUtils.isDkeTaskMode(this.config)) {
            if (HttpSourceConnector.taskCount.decrementAndGet() <= 0) {
                // do something finally if needed
            }
            log.info("Waiting to exit for offset committing ...");
            Utils.sleep(config.getTaskExitWait());
            throw new ConnectException(ExitUtils.MSG_DONE);// force task stop
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // 1) 每次分页迭代开始需要重置翻页参数；2）快照阶段不使用间隔等待
        if (!offset.isPaginating() && !offset.isSnapshoting()) {
            checkIfTaskDone();
            throttler.throttle(offset.getTimestamp().orElseGet(Instant::now));
        }

        offset = ScriptUtils.evalScript(config.getPollScriptPre(), offset);
        Pageable pageRequest = offset.getPageable().orElse(null);
        log.info("Requesting for offset {}", offset);
        HttpRequest request = requestFactory.createRequest(offset);

        HttpResponse response = execute(request);

        List<SourceRecord> records = responseParser.parse(response);

        List<SourceRecord> unseenRecords = recordSorter.sort(records).stream()
                .filter(recordFilterFactory.create(offset))
                .collect(toList());

        log.info("Requested {}/{} new records", unseenRecords.size(), records.size());

        List<Map<String, ?>> recordOffsets = extractOffsets(unseenRecords);
        confirmationWindow = new ConfirmationWindow<>(recordOffsets);

        if (config.hasPollScriptPost()) {
            offset = ScriptUtils.evalScript(config.getPollScriptPost(), offset);
        } else {
            offset = recordOffsets.stream().findFirst().map(map -> offset.updatePage(map, true, true, true)).orElse(offset);
            Page pageResult = offset.getPage().orElse(null);
            if (pageResult != null) {
                if (pageResult.hasNext()) { // 进入翻页过程，并置下一页
                    offset.setPaginating(true);
                    int nextPi = pageResult.nextPageable().getPageNumber() + offset.getPp();
                    if (nextPi == pageRequest.getPageNumber()) {
                        throw new ConnectException("请正确设置pp(首页序号)并清理Offset后重试");
                    }
                    offset.updatePi(nextPi);
                } else { // 退出翻页过程，退出快照过程
                    offset.setPaginating(false);
                    offset.setSnapshoting(false);
                }
            } else { // 没有翻页退出快照过程
                offset.setPaginating(false);
                offset.setSnapshoting(false);
            }
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
        Offset lastCommited = confirmationWindow.getLowWatermarkOffset().map(Offset::of).orElse(offset);
        log.debug("Offset committed: {}", lastCommited);
        if (!ConfigUtils.isDkeTaskMode(config)) {
            offset.update(lastCommited);
        }
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
