package com.github.castorm.kafka.connect.http.auth;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2021 Cástor Rodríguez
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

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.castorm.kafka.connect.http.auth.spi.HttpAuthenticator;
import com.github.castorm.kafka.connect.http.client.spi.HttpClient;
import com.github.castorm.kafka.connect.http.model.HttpRequest;
import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.request.template.spi.Template;
import com.github.castorm.kafka.connect.http.request.template.spi.TemplateFactory;
import com.github.castorm.kafka.connect.http.response.jackson.JacksonSerializer;
import edu.emory.mathcs.backport.java.util.Collections;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownHeaders;
import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownQueryParams;
import static com.github.castorm.kafka.connect.util.ScriptUtils.evalScript;
import static java.util.stream.Collectors.toMap;

public class TokenHttpAuthenticator implements HttpAuthenticator {

    private final Function<Map<String, ?>, TokenHttpAuthenticatorConfig> configFactory;

    private String method;

    private Template urlTpl;

    private Template headersTpl;

    private Template queryParamsTpl;

    private Template bodyTpl;

    private final JacksonSerializer serializer = new JacksonSerializer();
    private Map<String, JsonPointer> resPointers;
    private String resBodyName;
    private boolean needAuth = false;
    private String scriptPre;
    private String scriptPost;

    public TokenHttpAuthenticator() {
        this(TokenHttpAuthenticatorConfig::new);
    }

    public TokenHttpAuthenticator(Function<Map<String, ?>, TokenHttpAuthenticatorConfig> configFactory) {
        this.configFactory = configFactory;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        TokenHttpAuthenticatorConfig config = configFactory.apply(configs);
        TemplateFactory templateFactory = config.getTemplateFactory();

        needAuth = StringUtils.isNotEmpty(config.getUrl());
        method = config.getMethod();
        urlTpl = templateFactory.create(config.getUrl());
        headersTpl = templateFactory.create(config.getHeaders());
        queryParamsTpl = templateFactory.create(config.getQueryParams());
        bodyTpl = templateFactory.create(config.getBody());

        resPointers = config.getResPointers();
        resBodyName = config.getResBodyName();
        scriptPre = config.getScriptPre();
        scriptPost = config.getScriptPost();
    }

    HttpRequest createRequest(Offset offset) {
        return HttpRequest.builder()
                .method(HttpRequest.HttpMethod.valueOf(method))
                .url(urlTpl.apply(offset))
                .headers(breakDownHeaders(headersTpl.apply(offset)))
                .queryParams(breakDownQueryParams(queryParamsTpl.apply(offset)))
                .body(bodyTpl.apply(offset).getBytes())
                .build();
    }

    @SneakyThrows
    @Override
    public Offset authenticate(HttpClient client, Offset offset) {
        if (needAuth) {
            offset = evalScript(scriptPre, offset);
            HttpRequest request = createRequest(offset);
            HttpResponse response = client.execute(request);
            if (!CollectionUtils.isEmpty(resPointers)) {
                JsonNode jsonBody = serializer.deserialize(response.getBody());
                offset.update(resPointers.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, entry -> serializer.getObjectAt(jsonBody, entry.getValue()).asText())));
            } else if (StringUtils.isNotEmpty(resBodyName)) {
                offset.update(Collections.singletonMap(resBodyName, new String(response.getBody())));
            }
            offset = evalScript(scriptPost, offset);
        }
        return offset;
    }

    @Override
    public Optional<String> getAuthorizationHeader() {
        return Optional.empty();
    }
}
