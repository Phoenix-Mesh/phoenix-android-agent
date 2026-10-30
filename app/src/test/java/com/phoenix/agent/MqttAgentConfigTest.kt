// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class MqttAgentConfigTest {

    private fun normalized(input: String) = MqttAgentConfig.normalizeBrokerUri(input)

    private fun rejected(input: String): String =
        try {
            MqttAgentConfig.normalizeBrokerUri(input)
            fail("$input should have been rejected")
            ""
        } catch (e: IllegalArgumentException) {
            e.message ?: ""
        }

    @Test
    fun aBareHostOrHostAndPortMeansTls() {
        assertEquals("ssl://192.168.1.10:8883", normalized("192.168.1.10"))
        assertEquals("ssl://broker.lan:9883", normalized(" broker.lan:9883 "))
        assertEquals("ssl://broker.lan:8883", normalized("mqtts://broker.lan"))
        assertEquals("ssl://broker.lan:8883", normalized("SSL://broker.lan:8883/"))
    }

    @Test
    fun plainTcpIsOnlyForABrokerOnThisDevice() {
        assertEquals("tcp://localhost:1883", normalized("tcp://localhost"))
        assertEquals("tcp://127.0.0.1:1884", normalized("tcp://127.0.0.1:1884"))
        assertTrue(rejected("tcp://192.168.1.10:1883").contains("ssl://"))
    }

    @Test
    fun malformedAddressesAreRejected() {
        rejected("")
        rejected("http://broker.lan")
        rejected("ssl://broker.lan:8883/path")
        rejected("ssl://user@broker.lan:8883")
        rejected("ssl://bad_host_name:8883")
    }

    @Test
    fun thePasswordIsRequiredAndABlankTokenMeansNone() {
        try {
            MqttAgentConfig("broker.lan", password = "")
            fail("an empty password should be rejected")
        } catch (expected: IllegalArgumentException) {
            // fine
        }

        assertNull(MqttAgentConfig("broker.lan", "pw", enrollmentToken = "   ").enrollmentToken)
        assertEquals("tok", MqttAgentConfig("broker.lan", "pw", enrollmentToken = " tok ").enrollmentToken)
    }

    @Test(expected = IllegalArgumentException::class)
    fun aNegativeValidationHoldIsRejected() {
        MqttAgentConfig("broker.lan", "pw", validationHoldSeconds = -1)
    }

    @Test
    fun secretsNeverAppearInToString() {
        val text = MqttAgentConfig("broker.lan", "s3cret-pass", enrollmentToken = "enroll-me").toString()

        assertFalse(text.contains("s3cret-pass"))
        assertFalse(text.contains("enroll-me"))
        assertTrue(text.contains("ssl://broker.lan:8883"))
    }
}
