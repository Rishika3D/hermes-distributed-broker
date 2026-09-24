package io.hermes.core.broker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Compact string encodings for structured values carried in wire-frame
 * headers: partition lists, offset maps and lag reports. Topic and group
 * names therefore must not contain {@code , ; :} characters.
 */
public final class WireEncoding {

    private WireEncoding() {
    }

    public static String encodePartitions(List<Integer> partitions) {
        StringJoiner joiner = new StringJoiner(",");
        partitions.forEach(p -> joiner.add(Integer.toString(p)));
        return joiner.toString();
    }

    public static List<Integer> decodePartitions(String encoded) {
        List<Integer> partitions = new ArrayList<>();
        if (encoded != null && !encoded.isBlank()) {
            for (String part : encoded.split(",")) {
                partitions.add(Integer.parseInt(part));
            }
        }
        return partitions;
    }

    public static String encodeOffsets(Map<Integer, Long> offsets) {
        StringJoiner joiner = new StringJoiner(",");
        offsets.forEach((partition, offset) -> joiner.add(partition + ":" + offset));
        return joiner.toString();
    }

    public static Map<Integer, Long> decodeOffsets(String encoded) {
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        if (encoded != null && !encoded.isBlank()) {
            for (String entry : encoded.split(",")) {
                String[] parts = entry.split(":");
                offsets.put(Integer.parseInt(parts[0]), Long.parseLong(parts[1]));
            }
        }
        return offsets;
    }

    public static String encodeLag(List<Map<String, Object>> entries) {
        StringJoiner joiner = new StringJoiner(";");
        for (Map<String, Object> entry : entries) {
            joiner.add(entry.get("group") + "," + entry.get("topic") + "," + entry.get("partition")
                    + "," + entry.get("committed") + "," + entry.get("endOffset")
                    + "," + entry.get("lag") + "," + entry.get("generation") + "," + entry.get("members"));
        }
        return joiner.toString();
    }

    public static List<Map<String, Object>> decodeLag(String encoded) {
        List<Map<String, Object>> entries = new ArrayList<>();
        if (encoded == null || encoded.isBlank()) {
            return entries;
        }
        for (String line : encoded.split(";")) {
            String[] parts = line.split(",");
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("group", parts[0]);
            entry.put("topic", parts[1]);
            entry.put("partition", Integer.parseInt(parts[2]));
            entry.put("committed", Long.parseLong(parts[3]));
            entry.put("endOffset", Long.parseLong(parts[4]));
            entry.put("lag", Long.parseLong(parts[5]));
            entry.put("generation", Integer.parseInt(parts[6]));
            entry.put("members", Integer.parseInt(parts[7]));
            entries.add(entry);
        }
        return entries;
    }
}
