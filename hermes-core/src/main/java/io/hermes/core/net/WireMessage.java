package io.hermes.core.net;

import io.hermes.core.model.Message;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One frame on the wire: a type, string headers for scalar fields, and an
 * optional batch of records. Kept deliberately simple so the whole protocol
 * fits in one codec.
 */
public final class WireMessage {

    private final FrameType type;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private final List<Message> records = new ArrayList<>();

    private WireMessage(FrameType type) {
        this.type = type;
    }

    public static WireMessage of(FrameType type) {
        return new WireMessage(type);
    }

    public static WireMessage error(String reason) {
        return of(FrameType.ERROR).header("reason", reason);
    }

    public WireMessage header(String key, String value) {
        headers.put(key, value == null ? "" : value);
        return this;
    }

    public WireMessage header(String key, long value) {
        return header(key, Long.toString(value));
    }

    public WireMessage record(Message message) {
        records.add(message);
        return this;
    }

    public WireMessage records(List<Message> messages) {
        records.addAll(messages);
        return this;
    }

    public FrameType type() {
        return type;
    }

    public String get(String key) {
        return headers.get(key);
    }

    public int getInt(String key) {
        return Integer.parseInt(headers.get(key));
    }

    public long getLong(String key) {
        return Long.parseLong(headers.get(key));
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(headers.get(key));
    }

    public Map<String, String> headers() {
        return headers;
    }

    public List<Message> records() {
        return records;
    }

    public boolean isError() {
        return type == FrameType.ERROR;
    }

    @Override
    public String toString() {
        return type + " " + headers + (records.isEmpty() ? "" : " +" + records.size() + " records");
    }
}
