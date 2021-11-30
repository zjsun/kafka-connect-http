package com.github.castorm.kafka.connect.http.auth;

import com.fasterxml.jackson.core.JsonPointer;
import com.github.castorm.kafka.connect.http.request.template.freemarker.FreeMarkerTemplateFactory;
import com.github.castorm.kafka.connect.http.request.template.spi.TemplateFactory;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.AbstractMap;
import java.util.Map;

import static com.fasterxml.jackson.core.JsonPointer.compile;
import static com.github.castorm.kafka.connect.common.ConfigUtils.breakDownMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

@Getter
public class TokenHttpAuthenticatorConfig extends AbstractConfig {

    private static final String REQ_URL = "http.auth.url";
    private static final String REQ_METHOD = "http.auth.method";
    private static final String REQ_HEADERS = "http.auth.headers";
    private static final String REQ_QUERY_PARAMS = "http.auth.params";
    private static final String REQ_BODY = "http.auth.body";
    private static final String REQ_TEMPLATE_FACTORY = "http.auth.template.factory";

    private static final String RES_POINTER = "http.auth.response.pointer";
    private static final String RES_BODY_NAME = "http.auth.response.body.name";

    private final String url;

    private final String method;

    private final String headers;

    private final String queryParams;

    private final String body;

    private final TemplateFactory templateFactory;

    private final Map<String, JsonPointer> resPointers;
    private final String resBodyName;

    TokenHttpAuthenticatorConfig(Map<String, ?> originals) {
        super(config(), originals);
        url = getString(REQ_URL);
        method = getString(REQ_METHOD);
        headers = getString(REQ_HEADERS);
        queryParams = getString(REQ_QUERY_PARAMS);
        body = getString(REQ_BODY);
        templateFactory = getConfiguredInstance(REQ_TEMPLATE_FACTORY, TemplateFactory.class);

        resPointers = breakDownMap(getString(RES_POINTER)).entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(entry.getKey(), compile(entry.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        resBodyName = getString(RES_BODY_NAME);
    }

    public static ConfigDef config() {
        return new ConfigDef()
                .define(REQ_URL, STRING, "", HIGH, "Auth Request URL Template")
                .define(REQ_METHOD, STRING, "GET", HIGH, "Auth Request Method Template")
                .define(REQ_HEADERS, STRING, "", MEDIUM, "Auth Request Headers Template")
                .define(REQ_QUERY_PARAMS, STRING, "", MEDIUM, "Auth Request Query Params Template")
                .define(REQ_BODY, STRING, "", LOW, "Auth Request Body Template")
                .define(REQ_TEMPLATE_FACTORY, CLASS, FreeMarkerTemplateFactory.class, LOW, "Auth Template Factory Class")
                .define(RES_POINTER, STRING, "", MEDIUM, "Auth Response JsonPointers")
                .define(RES_BODY_NAME, STRING, "", MEDIUM, "Auth Response Body Name")
                ;
    }
}
