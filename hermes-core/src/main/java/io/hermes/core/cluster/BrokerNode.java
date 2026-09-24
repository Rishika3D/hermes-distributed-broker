package io.hermes.core.cluster;

/** Static identity of one broker in the cluster: id plus its socket address. */
public record BrokerNode(int id, String host, int port) {

    /** Parses the {@code id@host:port} form used in peer lists. */
    public static BrokerNode parse(String spec) {
        String[] idAndAddress = spec.trim().split("@");
        if (idAndAddress.length != 2) {
            throw new IllegalArgumentException("invalid broker spec (want id@host:port): " + spec);
        }
        String[] hostAndPort = idAndAddress[1].split(":");
        if (hostAndPort.length != 2) {
            throw new IllegalArgumentException("invalid broker spec (want id@host:port): " + spec);
        }
        return new BrokerNode(Integer.parseInt(idAndAddress[0]), hostAndPort[0], Integer.parseInt(hostAndPort[1]));
    }
}
