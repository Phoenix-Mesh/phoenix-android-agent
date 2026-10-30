// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class MqttTlsTest {

    private companion object {
        const val DNS = 2
        const val IP = 7

        /** Test fixture only (self-signed, SANs DNS:broker.test and IP:10.0.0.5). */
        const val TEST_CERTIFICATE = """-----BEGIN CERTIFICATE-----
MIIBqTCCAU+gAwIBAgIUI205Po1D0T16duknhb0/sIrYayMwCgYIKoZIzj0EAwIw
GjEYMBYGA1UEAwwPUGhvZW5peCBUZXN0IENBMCAXDTI2MTAyMTA5NTgyMloYDzIx
MjYwOTI3MDk1ODIyWjAaMRgwFgYDVQQDDA9QaG9lbml4IFRlc3QgQ0EwWTATBgcq
hkjOPQIBBggqhkjOPQMBBwNCAASj4nMUZ1tkZ1uvaF6WK/S9P27sgCKW5u5ZfK+z
3iAq/z+bB7xX5zFfEULAscYCZn3rIV6GlGO68gNZHqM6V0uLo3EwbzAdBgNVHQ4E
FgQUOAnna5NBWQ4E0rydJkLJrQoka5MwHwYDVR0jBBgwFoAUOAnna5NBWQ4E0ryd
JkLJrQoka5MwDwYDVR0TAQH/BAUwAwEB/zAcBgNVHREEFTATggticm9rZXIudGVz
dIcECgAABTAKBggqhkjOPQQDAgNIADBFAiBERZTGRVqFXxM61bO1gGMIolX401rH
hcD9ZZguwzTGjwIhAPjU/rGOTSZOmEa/1jpqFvzrF8IbcCG/VO2FtgXZTl95
-----END CERTIFICATE-----"""
    }

    private fun matches(host: String, vararg sans: Pair<Int, String>) =
        TlsHostnameVerifier.matches(host, sans.toList())

    @Test
    fun aDnsNameMatchesExactlyIgnoringCaseAndATrailingDot() {
        assertTrue(matches("broker.lan", DNS to "broker.lan"))
        assertTrue(matches("Broker.LAN.", DNS to "broker.lan"))
        assertFalse(matches("other.lan", DNS to "broker.lan"))
        assertFalse(matches("broker.lan.evil.com", DNS to "broker.lan"))
    }

    @Test
    fun aWildcardStandsForExactlyOneLeftMostLabel() {
        assertTrue(matches("a.phoenix.lan", DNS to "*.phoenix.lan"))
        assertFalse(matches("a.b.phoenix.lan", DNS to "*.phoenix.lan"))
        assertFalse(matches("phoenix.lan", DNS to "*.phoenix.lan"))
        // Too broad to be honoured.
        assertFalse(matches("broker.lan", DNS to "*.lan"))
    }

    @Test
    fun anIpAddressMatchesOnlyAnIpSan() {
        assertTrue(matches("192.168.1.20", IP to "192.168.1.20"))
        assertFalse(matches("192.168.1.20", DNS to "192.168.1.20"))
        assertFalse(matches("192.168.1.21", IP to "192.168.1.20"))
        assertTrue(matches("[::1]", IP to "0:0:0:0:0:0:0:1"))
    }

    @Test
    fun nothingMatchesWithoutSans() {
        assertFalse(matches("broker.lan"))
        assertFalse(matches("", DNS to ""))
    }

    @Test
    fun aPemCertificateIsReadWithItsSubjectAltNames() {
        val certificate = MqttTls.parseCertificates(TEST_CERTIFICATE).single()

        assertEquals(
            listOf(DNS to "broker.test", IP to "10.0.0.5"),
            TlsHostnameVerifier.subjectAltNames(certificate)
        )
        assertTrue(MqttTls.describe(TEST_CERTIFICATE).startsWith("CN=Phoenix Test CA"))

        // A socket factory trusting just this CA can be built.
        MqttTls.socketFactory(TEST_CERTIFICATE)
    }

    @Test(expected = IllegalArgumentException::class)
    fun aFileWithoutACertificateIsRejected() {
        MqttTls.parseCertificates("this is not a certificate")
    }

    @Test(expected = IllegalArgumentException::class)
    fun anEmptyFileIsRejected() {
        MqttTls.parseCertificates("")
    }
}
