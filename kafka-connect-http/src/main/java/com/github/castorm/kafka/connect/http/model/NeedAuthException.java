package com.github.castorm.kafka.connect.http.model;

/**
 * @author Alex.Sun
 * @created 2021-12-03 22:28
 */
public class NeedAuthException extends RuntimeException {
    public NeedAuthException() {
    }

    public NeedAuthException(String message) {
        super(message);
    }
}
