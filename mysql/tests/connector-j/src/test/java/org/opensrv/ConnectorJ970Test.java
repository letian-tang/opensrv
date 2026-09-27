package org.opensrv;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectorJ970Test extends ProtocolTest {
    @Test void negativeTimeTextResultsAreCorrect() throws Exception {
        assertEquals("9.7.0", driverVersion);
        checkNegativeTextTimes(NEGATIVE_TIMES);
    }

    @Test void negativeBinaryTimeResultsAreCorrect() throws Exception {
        assertEquals("9.7.0", driverVersion);
        // Oracle fixed Bug #119863 / #38951042 in Connector/J 9.7.0.
        // Correct values are required; no fallback to legacy incorrect output.
        checkNegativeBinaryTimes(NEGATIVE_TIMES);
    }
}
