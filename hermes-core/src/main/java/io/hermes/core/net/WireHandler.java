package io.hermes.core.net;

/** Server-side dispatch: turns one inbound frame into its response frame. */
public interface WireHandler {

    WireMessage handle(WireMessage request);
}
