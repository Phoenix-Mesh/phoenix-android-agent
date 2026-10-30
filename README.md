# Phoenix Android Agent

The Android node agent for [Phoenix Core](https://github.com/Phoenix-Mesh/phoenix),
the lightweight edge orchestrator. The app registers a phone as a Phoenix node
and executes the work Phoenix Core assigns to it.

It speaks two transports: HTTP (v0.2, the original pull baseline) and
MQTT/TLS, which implements the Phoenix MQTT node-agent protocol, version 1.

## Documentation

- [Connecting the Android agent over MQTT/TLS](docs/mqtt-tls.md)
- [MQTT/TLS device validation runbook](docs/mqtt-tls-device-validation.md)

## Related repositories

- [Phoenix Core](https://github.com/Phoenix-Mesh/phoenix): the orchestrator.
- [Phoenix Linux Agent](https://github.com/Phoenix-Mesh/phoenix-linux-agent):
  the Linux node agent.

## License

This project is licensed under the
[Apache License, Version 2.0](LICENSE).
