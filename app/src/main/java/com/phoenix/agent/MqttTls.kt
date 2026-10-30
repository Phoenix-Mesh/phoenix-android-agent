// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import java.io.ByteArrayInputStream
import java.net.InetAddress
import java.security.KeyStore
import java.security.cert.CertificateException
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.Locale
import javax.net.ssl.HostnameVerifier
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLPeerUnverifiedException
import javax.net.ssl.SSLSession
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManagerFactory

/**
 * TLS for the broker connection: which CA to trust, and whether the broker's
 * certificate belongs to the host this device connects to.
 */
object MqttTls {

    /**
     * A socket factory trusting exactly the certificates in [caPem], or the
     * platform's CA store if [caPem] is null.
     *
     * @throws IllegalArgumentException if [caPem] holds no usable certificate
     */
    fun socketFactory(caPem: String?): SSLSocketFactory {
        if (caPem == null) {
            return SSLContext.getDefault().socketFactory
        }

        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null, null)
            parseCertificates(caPem).forEachIndexed { index, certificate ->
                setCertificateEntry("phoenix-ca-$index", certificate)
            }
        }

        val trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm())
        trust.init(keyStore)

        val context = SSLContext.getInstance("TLS")
        context.init(null, trust.trustManagers, null)
        return context.socketFactory
    }

    /**
     * The certificates in a PEM file.
     *
     * @throws IllegalArgumentException if there are none, or they cannot be read
     */
    fun parseCertificates(pem: String): List<X509Certificate> {
        val certificates = try {
            CertificateFactory.getInstance("X.509")
                .generateCertificates(ByteArrayInputStream(pem.trim().toByteArray(Charsets.US_ASCII)))
        } catch (e: CertificateException) {
            throw IllegalArgumentException("Not a PEM certificate file: ${e.message}", e)
        }

        val x509 = certificates.filterIsInstance<X509Certificate>()
        require(x509.isNotEmpty()) { "The file contains no certificate." }
        return x509
    }

    /** One line for the UI: the first certificate's subject and expiry. */
    fun describe(pem: String): String {
        val first = parseCertificates(pem).first()
        return "${first.subjectX500Principal.name} (valid until ${first.notAfter})"
    }
}

/**
 * Checks that the broker's certificate names the host this device connected
 * to, by its subject alternative names (DNS names and IP addresses).
 *
 * Paho 1.2.5 implements its own "HTTPS hostname verification" with an API
 * that does not exist before Android 7.0 and silently skips the check there.
 * This verifier runs after the handshake on every API level instead, and
 * behaves the same on the JVM, where it is unit-tested.
 *
 * The subject common name is not consulted (RFC 6125, and what current
 * browsers do); certificates from deploy/mosquitto/gen-certs.sh carry SANs.
 */
object TlsHostnameVerifier : HostnameVerifier {

    private const val SAN_DNS = 2
    private const val SAN_IP = 7
    private val IPV4 = Regex("\\d{1,3}(\\.\\d{1,3}){3}")

    override fun verify(host: String?, session: SSLSession?): Boolean {
        if (host == null || session == null) return false

        val leaf = try {
            session.peerCertificates.firstOrNull() as? X509Certificate
        } catch (e: SSLPeerUnverifiedException) {
            null
        } ?: return false

        return matches(host, subjectAltNames(leaf))
    }

    /** `(type, value)` pairs of the certificate's DNS and IP SANs. */
    fun subjectAltNames(certificate: X509Certificate): List<Pair<Int, String>> =
        try {
            certificate.subjectAlternativeNames.orEmpty().mapNotNull { entry ->
                val type = entry.getOrNull(0) as? Int
                val value = entry.getOrNull(1) as? String
                if (type == null || value == null) null else type to value
            }
        } catch (e: java.security.cert.CertificateParsingException) {
            emptyList()
        }

    /** True if [host] equals one of [sans] (a DNS name, a `*.` wildcard, or an IP). */
    fun matches(host: String, sans: List<Pair<Int, String>>): Boolean {
        val name = normalize(host)
        if (name.isEmpty()) return false

        return if (isIpLiteral(name)) {
            sans.any { (type, value) -> type == SAN_IP && sameAddress(name, value) }
        } else {
            sans.any { (type, value) -> type == SAN_DNS && dnsMatches(name, normalize(value)) }
        }
    }

    private fun normalize(host: String): String =
        host.trim().removePrefix("[").removeSuffix("]").removeSuffix(".").lowercase(Locale.ROOT)

    private fun isIpLiteral(host: String) = IPV4.matches(host) || host.contains(':')

    /** IP literals are parsed, never resolved, so "::1" equals "0:0:0:0:0:0:0:1". */
    private fun sameAddress(host: String, san: String): Boolean {
        val candidate = normalize(san)
        if (!isIpLiteral(candidate)) return false
        return try {
            InetAddress.getByName(host).address.contentEquals(InetAddress.getByName(candidate).address)
        } catch (e: Exception) {
            false
        }
    }

    /**
     * Exact match, or a wildcard that stands for exactly one left-most label
     * of a name with at least three labels (`*.lan` matches nothing).
     */
    private fun dnsMatches(host: String, pattern: String): Boolean {
        if (pattern.isEmpty()) return false
        if (!pattern.startsWith("*.")) return host == pattern

        val suffix = pattern.substring(1) // ".example.com"
        if (suffix.count { it == '.' } < 2) return false
        if (!host.endsWith(suffix)) return false

        val label = host.substring(0, host.length - suffix.length)
        return label.isNotEmpty() && !label.contains('.')
    }
}
