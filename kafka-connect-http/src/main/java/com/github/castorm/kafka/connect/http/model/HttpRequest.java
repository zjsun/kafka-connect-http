package com.github.castorm.kafka.connect.http.model;

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

import lombok.Builder;
import lombok.Builder.Default;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;
import java.util.Map;

import static com.github.castorm.kafka.connect.http.model.HttpRequest.HttpMethod.GET;
import static java.util.Collections.emptyMap;

@Value
@Builder
public class HttpRequest {

    @Default
    HttpMethod method = GET;

    String url;

    @Default
    Map<String, List<String>> queryParams = emptyMap();

    @Default
    Map<String, List<String>> headers = emptyMap();

    byte[] body;

    @SneakyThrows
    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("method", method)
                .append("url", url)
                .append("queryParams", queryParams)
                .append("headers", headers)
                .append("body", ArrayUtils.isEmpty(body) ? null : new String(body, "UTF-8"))
                .toString();
    }

    public enum HttpMethod {
        GET, HEAD, POST, PUT, PATCH
    }
}
