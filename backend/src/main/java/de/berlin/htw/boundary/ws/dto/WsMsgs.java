package de.berlin.htw.boundary.ws.dto;

public final class WsMsgs {
    public static final class Sub {
        public String action;
        public String symbolId;
        public String venueId;
        public String channel;
        public Integer window;
    }

    public static final class Pong {
        public String type = "pong";
    }
}