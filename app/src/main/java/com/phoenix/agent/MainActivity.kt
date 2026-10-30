// SPDX-License-Identifier: Apache-2.0

package com.phoenix.agent

import android.graphics.Color
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.view.View
import android.view.WindowManager
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.RadioGroup
import android.widget.TextView
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.ViewCompat
import androidx.core.view.WindowCompat
import androidx.core.view.WindowInsetsCompat

/**
 * Single-screen UI for the Phoenix node agent: HTTP (the v0.2 baseline) or
 * MQTT/TLS.
 *
 * The status badge shows this device's connection, never Phoenix lifecycle
 * state: whether a node is ONLINE, DEGRADED or OFFLINE is decided by Phoenix
 * Core from heartbeats, and shown there.
 */
class MainActivity : AppCompatActivity() {

    private companion object {
        const val MAX_EVENT_LINES = 30
        const val MAX_CA_FILE_BYTES = 64 * 1024

        val COLOR_CONNECTED = Color.rgb(106, 233, 165)
        val COLOR_CONNECTING = Color.rgb(255, 210, 105)
        val COLOR_WARNING = Color.rgb(255, 195, 92)
        val COLOR_ERROR = Color.rgb(255, 115, 115)
        val COLOR_DISCONNECTED = Color.rgb(150, 153, 170)
    }

    private lateinit var statusText: TextView
    private lateinit var deviceNameText: TextView
    private lateinit var deviceInfoText: TextView
    private lateinit var deviceIdText: TextView
    private lateinit var cardCores: TextView
    private lateinit var cardRam: TextView
    private lateinit var transportGroup: RadioGroup
    private lateinit var coreUrlInput: EditText
    private lateinit var mqttSettings: LinearLayout
    private lateinit var brokerUrlInput: EditText
    private lateinit var brokerUserText: TextView
    private lateinit var brokerPasswordInput: EditText
    private lateinit var enrollmentTokenInput: EditText
    private lateinit var caButton: Button
    private lateinit var caText: TextView
    private lateinit var validationHoldInput: EditText
    private lateinit var connectionHint: TextView
    private lateinit var connectButton: Button
    private lateinit var taskStateText: TextView
    private lateinit var taskDetailText: TextView
    private lateinit var logText: TextView

    private lateinit var settings: AgentSettings
    private lateinit var identity: DeviceIdentity
    private lateinit var httpAgent: PhoenixNodeAgent
    private lateinit var mqttAgent: PhoenixMqttNodeAgent

    /** The agent the user last connected, whichever transport. */
    private var activeAgent: NodeAgent? = null

    private val eventLines = ArrayDeque<String>()

    private val pickCaCertificate =
        registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
            if (uri != null) loadCaCertificate(uri)
        }

    /**
     * Targeting a recent SDK makes Android draw the app edge to edge, so the
     * status bar, navigation bar and display cutout overlap the content. Pad
     * the scroll view by those insets (they are zero where the system already
     * reserves the space) and keep the bar icons light on the dark background.
     */
    private fun applySystemBarInsets() {
        val root = findViewById<View>(R.id.rootScroll)
        val left = root.paddingLeft
        val top = root.paddingTop
        val right = root.paddingRight
        val bottom = root.paddingBottom
        ViewCompat.setOnApplyWindowInsetsListener(root) { view, insets ->
            val bars = insets.getInsets(
                WindowInsetsCompat.Type.systemBars() or WindowInsetsCompat.Type.displayCutout()
            )
            view.setPadding(left + bars.left, top + bars.top, right + bars.right, bottom + bars.bottom)
            insets
        }
        WindowCompat.getInsetsController(window, root).apply {
            isAppearanceLightStatusBars = false
            isAppearanceLightNavigationBars = false
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)
        applySystemBarInsets()
        bindViews()

        settings = AgentSettings(this)
        identity = DeviceIdentity(this)
        val telemetry = DeviceTelemetry(this)

        val onLog: (String) -> Unit = { message -> runOnUiThread { handleAgentMessage(message) } }
        httpAgent = PhoenixNodeAgent(identity, telemetry, PhoenixHttpClient(), onLog)
        mqttAgent = PhoenixMqttNodeAgent(identity, telemetry, onLog)

        renderDevice(telemetry)
        renderSettings()
        setDisconnectedState()

        transportGroup.setOnCheckedChangeListener { _, _ -> showTransportSettings() }

        caButton.setOnClickListener { pickCaCertificate.launch("*/*") }
        caButton.setOnLongClickListener {
            settings.caCertificatePem = null
            renderCaCertificate()
            appendEvent("CA certificate cleared; the platform CA store will be trusted.")
            true
        }

        connectButton.setOnClickListener {
            if (activeAgent?.running == true) {
                activeAgent?.disconnect()
                setDisconnectedState()
            } else if (isMqttSelected()) {
                connectOverMqtt()
            } else {
                connectOverHttp()
            }
        }
    }

    override fun onDestroy() {
        if (::httpAgent.isInitialized) httpAgent.disconnect()
        if (::mqttAgent.isInitialized) mqttAgent.disconnect()
        super.onDestroy()
    }

    private fun bindViews() {
        statusText = findViewById(R.id.statusText)
        deviceNameText = findViewById(R.id.deviceNameText)
        deviceInfoText = findViewById(R.id.deviceInfoText)
        deviceIdText = findViewById(R.id.deviceIdText)
        cardCores = findViewById(R.id.cardCores)
        cardRam = findViewById(R.id.cardRam)
        transportGroup = findViewById(R.id.transportGroup)
        coreUrlInput = findViewById(R.id.coreUrlInput)
        mqttSettings = findViewById(R.id.mqttSettings)
        brokerUrlInput = findViewById(R.id.brokerUrlInput)
        brokerUserText = findViewById(R.id.brokerUserText)
        brokerPasswordInput = findViewById(R.id.brokerPasswordInput)
        enrollmentTokenInput = findViewById(R.id.enrollmentTokenInput)
        caButton = findViewById(R.id.caButton)
        caText = findViewById(R.id.caText)
        validationHoldInput = findViewById(R.id.validationHoldInput)
        connectionHint = findViewById(R.id.connectionHint)
        connectButton = findViewById(R.id.connectButton)
        taskStateText = findViewById(R.id.taskStateText)
        taskDetailText = findViewById(R.id.taskDetailText)
        logText = findViewById(R.id.logText)
    }

    private fun renderDevice(telemetry: DeviceTelemetry) {
        deviceNameText.text = telemetry.displayName
        deviceInfoText.text =
            "Android ${Build.VERSION.RELEASE}  •  API ${Build.VERSION.SDK_INT}\n" +
                "${telemetry.architecture}  •  ${telemetry.cpuCores} CPU cores"
        deviceIdText.text = "Node identity\n${identity.deviceId}"
        cardCores.text = telemetry.cpuCores.toString()
        cardRam.text = "${telemetry.totalRamMb} MB"
    }

    private fun renderSettings() {
        coreUrlInput.setText(settings.coreUrl)
        brokerUrlInput.setText(settings.brokerUri)
        brokerPasswordInput.setText(settings.brokerPassword)
        enrollmentTokenInput.setText(settings.enrollmentToken)
        validationHoldInput.setText("0")
        brokerUserText.text = "Broker user name: ${identity.deviceId}"
        renderCaCertificate()

        transportGroup.check(
            if (settings.transport == AgentSettings.Transport.MQTT) R.id.transportMqtt else R.id.transportHttp
        )
        showTransportSettings()
    }

    private fun renderCaCertificate() {
        val pem = settings.caCertificatePem
        caText.text = if (pem == null) {
            "No CA chosen: the platform CA store is trusted."
        } else {
            try {
                "CA: ${MqttTls.describe(pem)}\n(long-press the button to clear)"
            } catch (e: IllegalArgumentException) {
                "Stored CA certificate is unusable: ${e.message}"
            }
        }
    }

    private fun isMqttSelected() = transportGroup.checkedRadioButtonId == R.id.transportMqtt

    private fun showTransportSettings() {
        val mqtt = isMqttSelected()
        coreUrlInput.visibility = if (mqtt) View.GONE else View.VISIBLE
        mqttSettings.visibility = if (mqtt) View.VISIBLE else View.GONE
        connectionHint.text = if (mqtt) {
            "Connect through the MQTT broker over TLS. The broker account's user name must be this device's identity."
        } else {
            "Enter the address of the Phoenix Core on your local network."
        }
    }

    private fun setInputsEnabled(enabled: Boolean) {
        for (i in 0 until transportGroup.childCount) {
            transportGroup.getChildAt(i).isEnabled = enabled
        }
        for (view in listOf<View>(
            coreUrlInput, brokerUrlInput, brokerPasswordInput, enrollmentTokenInput, caButton, validationHoldInput
        )) {
            view.isEnabled = enabled
        }
    }

    // ------------------------------------------------------------------
    // connecting
    // ------------------------------------------------------------------

    private fun connectOverHttp() {
        val url = coreUrlInput.text.toString().trim().removeSuffix("/")

        if (url.isBlank()) {
            appendEvent("Enter a Phoenix Core address first.")
            showTask("CORE ADDRESS REQUIRED", "Example: http://192.168.1.10:8080")
            return
        }

        settings.transport = AgentSettings.Transport.HTTP
        settings.coreUrl = url

        beginConnecting("Connecting to $url")
        activeAgent = httpAgent

        httpAgent.connect(
            coreUrl = url,
            // The agent logs "Registered with Phoenix.", which updates the UI.
            onConnected = {},
            onFailed = { error -> runOnUiThread { connectionFailed(error) } }
        )
    }

    private fun connectOverMqtt() {
        val holdSeconds = validationHoldInput.text.toString().trim().ifEmpty { "0" }.toIntOrNull()

        val config = try {
            require(holdSeconds != null) { "The validation hold must be a whole number of seconds." }
            MqttAgentConfig(
                brokerUri = brokerUrlInput.text.toString(),
                password = brokerPasswordInput.text.toString(),
                enrollmentToken = enrollmentTokenInput.text.toString(),
                caCertificatePem = settings.caCertificatePem,
                validationHoldSeconds = holdSeconds
            )
        } catch (e: IllegalArgumentException) {
            val message = e.message ?: "Invalid MQTT settings."
            appendEvent(message)
            showTask("CHECK MQTT SETTINGS", message)
            return
        }

        settings.transport = AgentSettings.Transport.MQTT
        settings.brokerUri = config.brokerUri
        settings.brokerPassword = config.password
        settings.enrollmentToken = config.enrollmentToken ?: ""
        brokerUrlInput.setText(config.brokerUri)

        beginConnecting("Connecting to ${config.brokerUri} over MQTT/TLS")
        if (config.validationHoldSeconds > 0) {
            appendEvent("Validation hold: every task waits ${config.validationHoldSeconds} s before it runs.")
        }
        activeAgent = mqttAgent

        mqttAgent.connect(
            config,
            // The agent logs "Registered with Phoenix.", which updates the UI.
            onConnected = {},
            onFailed = { error -> runOnUiThread { connectionFailed(error) } }
        )
    }

    private fun beginConnecting(message: String) {
        setConnectingState()
        setInputsEnabled(false)
        keepScreenOn(true)
        showTask("CONNECTING", "Registering this device with Phoenix Core...")
        appendEvent(message)
    }

    private fun connectionFailed(error: String) {
        setFailedState()
        setInputsEnabled(true)
        keepScreenOn(false)
        showTask("CONNECTION FAILED", error)
        appendEvent(error)
    }

    /** While connected the screen stays on, so Android does not suspend the agent mid-validation. */
    private fun keepScreenOn(on: Boolean) {
        if (on) {
            window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        } else {
            window.clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        }
    }

    private fun loadCaCertificate(uri: Uri) {
        try {
            val bytes = contentResolver.openInputStream(uri)?.use { it.readBytes() }
                ?: throw IllegalArgumentException("The file could not be opened.")
            require(bytes.size <= MAX_CA_FILE_BYTES) { "The file is too large for a CA certificate." }

            val pem = String(bytes, Charsets.US_ASCII)
            val description = MqttTls.describe(pem) // throws if it holds no certificate

            settings.caCertificatePem = pem
            renderCaCertificate()
            appendEvent("CA certificate set: $description")
        } catch (e: Exception) {
            appendEvent("Not a usable CA certificate: ${e.message}")
            showTask("CA CERTIFICATE REJECTED", e.message ?: "Choose the ca.pem file of your broker.")
        }
    }

    // ------------------------------------------------------------------
    // agent events
    // ------------------------------------------------------------------

    private fun handleAgentMessage(message: String) {
        appendEvent(message)
        val running = activeAgent?.running == true

        when {
            message.startsWith("Registered with Phoenix.") -> {
                setConnectedState()
                showTask("READY FOR TASKS", "Phoenix Core can dispatch work to this node.")
            }

            message.startsWith("Phoenix node id ") -> {
                val nodeId = message.removePrefix("Phoenix node id ").substringBefore(" ")
                deviceIdText.text = "Node identity\n${identity.deviceId}\nPhoenix node $nodeId"
            }

            message.startsWith("Heartbeat sent:") || message == "Reconnected to the broker." -> {
                if (running) setConnectedState()
            }

            message.startsWith("Broker connection lost:") -> {
                if (running) {
                    setWarningState()
                    taskDetailText.text = "Broker connection lost; reconnecting..."
                }
            }

            message.startsWith("Received task ") -> {
                val taskId = message.removePrefix("Received task ").substringBefore(",").trim()
                setConnectedState()
                showTask(
                    "EXECUTING",
                    if (taskId.isNotBlank()) "$taskId  •  Monte-Carlo workload"
                    else "Phoenix task execution in progress."
                )
            }

            message.startsWith("Holding task ") -> {
                showTask("HOLDING (VALIDATION)", message.removePrefix("Holding task "))
            }

            message.startsWith("Task ") && message.contains(" done:") -> {
                setConnectedState()
                showTask("TASK COMPLETE", "Execution completed successfully. Ready for more work.")
            }

            message.startsWith("Task ") && message.contains(" failed:") -> {
                showTask("TASK FAILED", message.substringAfter(" failed:").trim())
            }

            message.startsWith("Task ") && (message.contains(" stopped:") || message.contains(" interrupted:")) -> {
                showTask("TASK STOPPED", message.substringAfter(":").trim())
            }

            message.startsWith("Poll failed:") -> {
                showTask("TASK CHANNEL WARNING", message.removePrefix("Poll failed:").trim())
            }

            message.startsWith("Heartbeat failed:") -> {
                taskDetailText.text = "Heartbeat could not reach Phoenix Core."
                if (running) setWarningState()
            }

            message.startsWith("Agent stopped.") -> {
                setDisconnectedState()
                showTask("READY", "Connect to Phoenix Core to rejoin the mesh.")
            }
        }
    }

    // ------------------------------------------------------------------
    // presentation
    // ------------------------------------------------------------------

    private fun showTask(state: String, detail: String) {
        taskStateText.text = state
        taskDetailText.text = detail
    }

    private fun setStatus(label: String, color: Int, button: String, buttonEnabled: Boolean) {
        statusText.text = label
        statusText.setTextColor(color)
        connectButton.text = button
        connectButton.isEnabled = buttonEnabled
    }

    private fun setConnectingState() =
        setStatus("CONNECTING", COLOR_CONNECTING, "CONNECTING…", buttonEnabled = true)

    private fun setConnectedState() =
        setStatus("● CONNECTED", COLOR_CONNECTED, "DISCONNECT", buttonEnabled = true)

    private fun setWarningState() =
        setStatus("● CONNECTION ISSUE", COLOR_WARNING, "DISCONNECT", buttonEnabled = true)

    private fun setFailedState() =
        setStatus("● ERROR", COLOR_ERROR, "CONNECT TO PHOENIX", buttonEnabled = true)

    private fun setDisconnectedState() {
        setStatus("● DISCONNECTED", COLOR_DISCONNECTED, "CONNECT TO PHOENIX", buttonEnabled = true)
        setInputsEnabled(true)
        keepScreenOn(false)
        showTask("NOT CONNECTED", "Connect to Phoenix Core to join the edge mesh.")
    }

    private fun appendEvent(message: String) {
        if (message.isBlank()) {
            return
        }

        eventLines.addLast(message)
        while (eventLines.size > MAX_EVENT_LINES) {
            eventLines.removeFirst()
        }

        logText.text = eventLines.joinToString(separator = "\n")
    }
}
