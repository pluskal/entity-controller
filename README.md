[![License](https://img.shields.io/github/license/danobot/entity-controller.svg?style=flat-square)](https://github.com/danobot/entity-controller/blob/develop/COPYING)
[![Blog](https://img.shields.io/badge/blog-The%20Budget%20Smart%20Home-orange?style=flat-square)](https://danielbkr.net/?utm_source=github&utm_medium=badge&utm_campaign=entity-controller)
[![donate paypal](https://img.shields.io/badge/donate-PayPal-blue.svg?style=flat-square)](https://paypal.me/danielb160)
[![donate gofundme](https://img.shields.io/badge/donate-GoFundMe-orange?style=flat-square)](https://gofund.me/7a2487d5)


# :wave: Introduction
Entity Controller (EC) is an implementation of "When This, Then That for x amount of time" using a finite state machine that ensures basic automations do not interfere with the rest of your home automation setup. This component encapsulates common automation scenarios into a neat package that can be configured easily and reused throughout your home. Traditional automations would need to be duplicated _for each instance_ in your config. The use cases for this component are endless because you can use any entity as input and outputs (there is no restriction to motion sensors and lights).

[Entity Controller Documentation](https://danobot.github.io/ec-docs/)

## Installation
EC is available in HACS store. Once installed, add the the following to your `configuration.yaml`, replacing the values for `sensor` and `entity` with one of your own. Reboot your Home Assistant server and you should have a motion controlled light that turns off after 5 seconds.
```
motion_light:
  sensor: binary_sensor.living_room_motion
  entity: light.tv_led
  delay: 5
```
## :clapper: Video Demo
I created the following video to give a high-level overview of all EC features, how they work and how you can configure them for your use cases.

[![Video](images/video_thumbnail.png)](https://youtu.be/HJQrA6sFlPs)

## Support
Maintaining and improving this integration is very time consuming because of the sheer number of supported use cases. If you use this component in your home please donate a few dollars or check the issue tracker to help with the investigation of defects or the implementation of new features. I would be happy to receive your pull request.

[![donate paypal](https://img.shields.io/badge/donate-PayPal-blue.svg?style=flat-square)](https://paypal.me/danielb160)
[![donate gofundme](https://img.shields.io/badge/donate-GoFundMe-orange?style=flat-square)](https://gofund.me/7a2487d5)

# Contributions
All contributions are welcome, including raising issues. Expect to be involved in the resolution of any issues. 

The `close-issue` bot is ruthless. Please provide all requested information to allow me to help you.

---

# Configuration Reference

## Basic options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sensor` / `sensors` | entity id(s) | — | Motion/binary sensor(s) that trigger activation |
| `entity` / `entities` | entity id(s) | — | Entities to control (lights, switches, …) |
| `delay` | seconds | 180 | How long to stay active after the last trigger |

## Forced Sensors (`forced_sensors`)

Sensors listed under `forced_sensors` bypass the `blocked`, `constrained`, and `overridden` states and **immediately activate** the controller regardless of its current state. This is useful for panic buttons, manual overrides, or priority scenes where the normal blocking logic should be ignored.

```yaml
entity_controller:
  living_room:
    sensor: binary_sensor.pir
    entity: light.ceiling
    forced_sensors:
      - binary_sensor.panic_button   # always activates, even when overridden
```

The `forced_sensors` list accepts any Home Assistant entity id whose state changes to one of the sensor-on states (default: `on`, `playing`, `home`, `True`).

## HA Bus Event Sensors (`event_sensors`)

`event_sensors` accepts a list of **HA bus event type strings**. When any of those events fires on the event bus the controller treats it the same as a sensor turning on — transitioning from `idle`, `active_timer`, or `blocked` to `active`. Unlike `forced_sensors`, event sensors respect the `overridden` and `constrained` states.

```yaml
entity_controller:
  hallway:
    sensor: binary_sensor.door
    entity: light.hallway
    event_sensors:
      - my_custom_event          # fires when HA fires this bus event
      - zwave_js.value_updated   # or any other HA event type
```

Cancel callbacks are tracked automatically and cleaned up whenever the configuration is refreshed.

## Lux Constraint (`lux_entity` / `lux_threshold` / `lux_bright_states`)

Gate activation on measured room illuminance: motion turns the lights on **only while the room is dark enough**. Walking into an already-bright room no longer switches the lights on. Two sensor flavours are supported on the same `lux_entity`:

- **numeric sensors** (PIR-style lux): activation requires reading < `lux_threshold` (in whatever unit the sensor reports, typically lx);
- **string sensors** (radar/mmWave two-level `bright`/`dim`): activation is blocked while the state matches one of `lux_bright_states` — the light-level calibration lives in the sensor's own firmware.

```yaml
entity_controller:
  bedroom:                                  # numeric PIR lux
    sensor: binary_sensor.bedroom_occupancy
    entity: light.bedroom_led_strip
    lux_entity: sensor.bedroom_illuminance
    lux_threshold: 50                       # activate only below 50 lx

  living_room:                              # radar bright/dim
    sensor: binary_sensor.living_room_presence
    entity: light.living_room_wall
    lux_entity: sensor.living_room_presence_illumination
    lux_bright_states: ["bright"]
```

Both keys may be combined on one entity: numeric readings compare against the threshold, string states match the list.

The constraint deliberately gates **only** the `idle → active` transition that starts from a fully-off room. Everything else is unaffected:

- **timer resets** while active — the controlled light inflates the reading itself, so re-triggers must not be gated (otherwise the light would drop out mid-presence);
- the `blocked` bookkeeping paths (lights already on);
- `overrides`, `forced_sensors`, and the `activate` service (manual escape hatch).

A lux entity that is `unavailable`, missing, or otherwise unmatched (numeric on a bright-states-only config, or a string not in the list) **fails open**: activation is allowed and a warning is logged where appropriate. A dead sensor battery degrades to pre-lux behaviour instead of leaving the room dark. Setting `lux_entity` without any criterion — or a criterion without `lux_entity` — logs an error and disables the constraint.

When an activation is blocked, the EC entity records `lux_blocked_at` and `lux_at_last_block` attributes for diagnostics; `lux_entity` and `lux_threshold` are always shown as attributes when the constraint is active.

## State Persistence

EC now persists the `overridden` and `blocked` states across Home Assistant restarts using the built-in HA storage layer. On startup the saved state is re-validated against the current live entity states before being applied, so stale persisted states are silently discarded.

No configuration is required — persistence is enabled automatically.

## Block Timer Fix

Prior to v9.8.0, when the block timer expired while all state entities were already off, the controller was left stuck in the `blocked` state (issue #310). This has been fixed: the state machine now correctly transitions `blocked → idle` when the block timer expires and all state entities are off.

## Grace Period (`grace_period`)

**Problem:** Cloud and gateway integrations — such as [Tahoma / Somfy](https://www.home-assistant.io/integrations/tahoma/) — update entity states asynchronously by polling the cloud. When EC calls `light.turn_on`, the integration sends the command upstream and only confirms the new state seconds later through its next poll cycle. That delayed state-change event is emitted with a fresh HA context that has no relationship to the original EC service call. Because EC's self-suppression mechanism (`is_ignored_context`) looks for its own context id prefix, it misses the late-arriving event, fires `control()`, finds the state entity on, and transitions `active_timer → blocked`.

**Solution:** Set `grace_period` to a value (in seconds) that covers the integration's worst-case round-trip latency. EC will then ignore all state-entity changes that arrive within that window after any service call, preventing false `blocked` transitions.

```yaml
entity_controller:
  room_108:
    sensor: binary_sensor.108_motion_hs_portal_occupancy
    entity:
      - light.108_f
      - light.108_i
      - light.led_1
      - light.led_3
    delay: 300
    grace_period: 10   # covers Tahoma's ~5–6 s cloud round-trip latency
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `grace_period` | integer (seconds) | `null` (disabled) | Duration after each service call during which state-entity changes are ignored. Set to a value slightly above your integration's worst-case latency. |

**When to use it:** Only needed for integrations where state-change events arrive with a context unrelated to the original EC service call (cloud integrations, gateway bridges, etc.). Standard local integrations — where HA propagates the service-call context through to the state-change event — are handled correctly by the existing context check and do not need this option.

