package io.hermes.core.net;

import io.hermes.core.cluster.BrokerNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Connections to one peer broker, pooled so that every in-flight request gets
 * its own socket. This matters: requests here can nest (a forwarded produce
 * triggers replication back over the wire), and Raft traffic runs concurrently
 * with data traffic — serializing them on a single shared socket creates
 * cross-broker wait cycles that stall the cluster until timeouts fire.
 * A connection carries exactly one outstanding request at a time; failed
 * connections are discarded, healthy ones return to the pool.
 */
public final class PeerClient implements Closeable {

    private static final int CONNECT_TIMEOUT_MS = 1_000;
    private static final int READ_TIMEOUT_MS = 3_000;
    private static final int MAX_IDLE_CONNECTIONS = 4;

    private final BrokerNode node;
    private final Deque<Connection> idle = new ArrayDeque<>();
    private volatile boolean closed;

    public PeerClient(BrokerNode node) {
        this.node = node;
    }

    public WireMessage request(WireMessage message) throws IOException {
        Connection connection = borrow();
        try {
            WireMessage response = connection.roundTrip(message);
            giveBack(connection);
            return response;
        } catch (IOException e) {
            connection.close();
            throw e;
        }
    }

    private Connection borrow() throws IOException {
        synchronized (idle) {
            Connection pooled = idle.pollFirst();
            if (pooled != null) {
                return pooled;
            }
        }
        return Connection.open(node);
    }

    private void giveBack(Connection connection) {
        synchronized (idle) {
            if (!closed && idle.size() < MAX_IDLE_CONNECTIONS) {
                idle.addFirst(connection);
                return;
            }
        }
        connection.close();
    }

    public BrokerNode node() {
        return node;
    }

    @Override
    public void close() {
        closed = true;
        synchronized (idle) {
            idle.forEach(Connection::close);
            idle.clear();
        }
    }

    /** One TCP connection with strict request/response semantics. */
    private static final class Connection {

        private final Socket socket;
        private final DataInputStream in;
        private final DataOutputStream out;

        private Connection(Socket socket, DataInputStream in, DataOutputStream out) {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }

        static Connection open(BrokerNode node) throws IOException {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(node.host(), node.port()), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);
            return new Connection(socket,
                    new DataInputStream(new BufferedInputStream(socket.getInputStream())),
                    new DataOutputStream(new BufferedOutputStream(socket.getOutputStream())));
        }

        WireMessage roundTrip(WireMessage message) throws IOException {
            MessageCodec.encode(message, out);
            return MessageCodec.decode(in);
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best-effort teardown
            }
        }
    }
}
