package com.jpsbharat.p2pserver;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AppTest {
    @Test
    void appHasExpectedMessage() {
        App app = new App();
        assertEquals("p2pServer is ready.", app.getMessage());
    }
}
