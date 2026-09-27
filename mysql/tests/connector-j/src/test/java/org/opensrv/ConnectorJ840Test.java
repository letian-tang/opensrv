package org.opensrv;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectorJ840Test extends ProtocolTest {
    @Test void documentsConnectorJ840NegativeTextTimeLimitation() throws Exception {
        assertEquals("8.4.0", driverVersion);
        // The text decoder stores the sign only in hours: -0 becomes 0.
        // InternalTime also formats a negative single-digit hour without zero padding.
        // Characterization only, NOT negative TIME compatibility success.
        checkNegativeTextTimes(new String[]{
            "-25:02:03.123456", "00:00:00", "-838:59:58.999999",
            "00:00:00.000001", "00:02:03.456789", "-1:02:03",
            "-23:59:59.999999", "-24:00:00", "-838:59:59", "00:00:00", null
        });
    }

    @Test void documentsConnectorJ840NegativeBinaryTimeLimitation() throws Exception {
        assertEquals("8.4.0", driverVersion);
        // Characterization only, NOT negative TIME compatibility success.
        // The old decoder negates days but loses the sign for the remaining hours.
        checkNegativeBinaryTimes(new String[]{
            "-23:02:03.123456", "00:00:00", "-794:59:58.999999",
            "00:00:00.000001", "00:02:03.456789", "01:02:03",
            "23:59:59.999999", "-24:00:00", "-794:59:59", "00:00:00", null
        });
    }
}
