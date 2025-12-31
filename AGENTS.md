# Repository Guidelines

## Project Structure & Module Organization
- `components/igrill/`: main ESPHome component (C++ core in `igrill.cpp`/`igrill.h`, config glue in `sensor.py`).
- `components/igrill_ble_listener/`: BLE discovery helper for finding device MAC addresses.
- `full_example_*.yaml`: example ESPHome configurations for supported devices (Mini, V2, V3, Pulse 2000).
- `README.md`: setup, configuration options, and supported device list.

## Build, Test, and Development Commands
This repository is an ESPHome external component, so use the ESPHome CLI with a config file:
- `esphome compile full_example_V3.yaml` — validates config and builds firmware.
- `esphome run full_example_V3.yaml` — builds, uploads, and starts logs.
- `esphome logs full_example_V3.yaml` — attaches to device logs for troubleshooting.

## Coding Style & Naming Conventions
- C++: follow existing 2-space indentation and brace style in `components/igrill/igrill.cpp`.
- Python: 4-space indentation; constants use `CONF_*` names, config keys are snake_case.
- Sensor IDs follow existing patterns like `temperature_probe1` and `pulse_heating_actual1`.
- Keep changes aligned with ESPHome component patterns and APIs.

## Testing Guidelines
- No automated tests live in this repo.
- Validate changes by compiling a sample config and verifying logs on real hardware.
- Use verbose logging when debugging BLE: add `logger:\n  level: VERBOSE` to your YAML.

## Commit & Pull Request Guidelines
- Commit messages are short, imperative summaries (e.g., “Add V3 example”).
- PRs should describe the device model tested, ESPHome version, and include a minimal YAML snippet.
- If adding sensors or config keys, update `README.md` and the relevant `full_example_*.yaml`.

## Configuration & Device Notes
- BLE devices accept only one connection at a time; disconnect the mobile app before testing.
- Use `igrill_ble_listener` to discover the MAC address, then switch to `ble_client` + `sensor` config.
