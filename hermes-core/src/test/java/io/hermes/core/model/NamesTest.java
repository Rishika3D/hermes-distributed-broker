package io.hermes.core.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NamesTest {

    @Test
    void acceptsTypicalNames() {
        assertEquals("orders", Names.requireValid("orders", "topic"));
        assertEquals("demo-consumers", Names.requireValid("demo-consumers", "group"));
        assertEquals("v1.click_events", Names.requireValid("v1.click_events", "topic"));
    }

    @Test
    void rejectsWireProtocolSeparators() {
        // these characters are structural in the wire encoding and offsets WAL
        for (String bad : new String[]{"a,b", "a;b", "a:b", "a b", "a@b", "a/b", "a|b"}) {
            assertThrows(IllegalArgumentException.class, () -> Names.requireValid(bad, "topic"),
                    "should reject: " + bad);
        }
    }

    @Test
    void rejectsPathTraversalAndEmpty() {
        assertThrows(IllegalArgumentException.class, () -> Names.requireValid("..", "topic"));
        assertThrows(IllegalArgumentException.class, () -> Names.requireValid(".", "topic"));
        assertThrows(IllegalArgumentException.class, () -> Names.requireValid("", "topic"));
        assertThrows(IllegalArgumentException.class, () -> Names.requireValid(null, "topic"));
        assertThrows(IllegalArgumentException.class, () -> Names.requireValid("x".repeat(300), "topic"));
    }
}
