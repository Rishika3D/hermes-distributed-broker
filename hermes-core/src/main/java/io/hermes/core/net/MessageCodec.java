package io.hermes.core.net;

import io.hermes.core.model.Message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Binary encoding of {@link WireMessage} frames over a TCP stream:
 *
 * <pre>
 *   type        int8
 *   headerCount int32, then headerCount x (UTF key, UTF value)
 *   recordCount int32, then recordCount x (offset int64, timestamp int64,
 *                                          hasKey bool, [UTF key], UTF value)
 * </pre>
 */
public final class MessageCodec {

    private MessageCodec() {
    }

    public static void encode(WireMessage message, DataOutputStream out) throws IOException {
        out.writeByte(message.type().code());
        out.writeInt(message.headers().size());
        for (Map.Entry<String, String> header : message.headers().entrySet()) {
            out.writeUTF(header.getKey());
            out.writeUTF(header.getValue());
        }
        out.writeInt(message.records().size());
        for (Message record : message.records()) {
            out.writeLong(record.offset());
            out.writeLong(record.timestamp());
            out.writeBoolean(record.key() != null);
            if (record.key() != null) {
                out.writeUTF(record.key());
            }
            out.writeUTF(record.value());
        }
        out.flush();
    }

    public static WireMessage decode(DataInputStream in) throws IOException {
        FrameType type = FrameType.fromCode(in.readByte());
        WireMessage message = WireMessage.of(type);
        int headerCount = in.readInt();
        for (int i = 0; i < headerCount; i++) {
            message.header(in.readUTF(), in.readUTF());
        }
        int recordCount = in.readInt();
        for (int i = 0; i < recordCount; i++) {
            long offset = in.readLong();
            long timestamp = in.readLong();
            String key = in.readBoolean() ? in.readUTF() : null;
            String value = in.readUTF();
            message.record(new Message(offset, timestamp, key, value));
        }
        return message;
    }
}
