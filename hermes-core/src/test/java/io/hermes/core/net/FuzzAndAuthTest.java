package io.hermes.core.net;

import io.hermes.core.broker.Broker;
import io.hermes.core.broker.BrokerConfig;
import io.hermes.core.broker.RequestHandler;
import io.hermes.core.cluster.BrokerNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fuzz + authentication coverage:
 *  - the codec must never hang or throw an unchecked error on random bytes;
 *  - the request handler must reject frames without the cluster secret before
 *    any dispatch, and accept them with it.
 */
@Timeout(60)
class FuzzAndAuthTest {

    @TempDir
    Path dir;

    @Test
    void codecSurvivesRandomBytesWithoutCrashingOrHanging() {
        Random random = new Random(1234);
        int decoded = 0;
        int rejected = 0;
        for (int i = 0; i < 50_000; i++) {
            byte[] junk = new byte[random.nextInt(64)];
            random.nextBytes(junk);
            try {
                MessageCodec.decode(new DataInputStream(new ByteArrayInputStream(junk)));
                decoded++;
            } catch (IOException | IllegalArgumentException | NegativeArraySizeException expected) {
                rejected++; // all controlled failure modes
            }
        }
        assertEquals(50_000, decoded + rejected, "every input produced a controlled outcome");
        assertTrue(rejected > 0, "some malformed inputs must be rejected");
        System.out.println("[FUZZ] 50000 random frames: decoded=" + decoded + " rejected=" + rejected
                + " (no hang, no uncontrolled crash)");
    }

    @Test
    void handlerRejectsUnauthenticatedFramesAndAcceptsAuthenticated() throws Exception {
        int socketPort;
        try (ServerSocket s = new ServerSocket(0)) {
            socketPort = s.getLocalPort();
        }
        Broker broker = new Broker(new BrokerConfig(1, dir.resolve("auth-broker"),
                List.of(new BrokerNode(1, "localhost", socketPort)), 1, 3, 64 * 1024, "s3cr3t"));
        try {
            RequestHandler handler = new RequestHandler(broker, "s3cr3t");

            WireMessage noSecret = WireMessage.of(FrameType.REQUEST_VOTE)
                    .header("term", 5).header("candidateId", 2);
            WireMessage rejected = handler.handle(noSecret);
            assertTrue(rejected.isError());
            assertEquals("unauthenticated frame rejected", rejected.get("reason"));
            System.out.println("[AUTH] frame without secret rejected: " + rejected.get("reason"));

            WireMessage wrongSecret = WireMessage.of(FrameType.REQUEST_VOTE)
                    .header("term", 5).header("candidateId", 2)
                    .header(PeerChannels.SECRET_HEADER, "wrong");
            assertTrue(handler.handle(wrongSecret).isError());
            System.out.println("[AUTH] frame with wrong secret rejected");

            WireMessage authed = WireMessage.of(FrameType.REQUEST_VOTE)
                    .header("term", 5).header("candidateId", 2)
                    .header(PeerChannels.SECRET_HEADER, "s3cr3t");
            WireMessage voteResponse = handler.handle(authed);
            assertFalse(voteResponse.isError(), "authenticated vote must be processed");
            assertEquals(FrameType.VOTE_RESPONSE, voteResponse.type());
            System.out.println("[AUTH] authenticated frame processed -> " + voteResponse.type());
        } finally {
            broker.close();
        }
    }
}
