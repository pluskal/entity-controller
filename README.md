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

## Entity-Driven Night Mode (`night_mode: entity` / `entity_states` / `entities`)

`night_mode` historically switched to alternate service parameters (dimmer
brightness, shorter delay) inside a fixed time window. It can now also be
driven by the **state of an entity** — typically a house-mode
`input_select` — and can swap the **target entities** themselves:

```yaml
entity_controller:
  ec_bedroom:
    sensors:
      - binary_sensor.bedroom_motion
    entities:
      - light.bedroom_wardrobe       # day/evening target
    night_mode:
      entity: input_select.house_mode
      entity_states: [vecerka, noc]  # these states mean "night"
      entities:
        - light.bedroom_bed          # night target replaces the day set
      delay: 120
```

Semantics:

- `entity` + `entity_states` declare night whenever the entity's state matches
  one of the listed strings. `start_time`/`end_time` are now optional; if both
  a window and an entity are configured, **whichever says night wins**.
  At least one of `entity` / `start_time` must be present.
- `entities` (optional) replaces the controller's target set while night mode
  is active. The set is chosen **at activation time** and remembered: if the
  mode flips while the timer runs, the eventual turn-off still targets
  whatever was turned on — no orphaned lights.
- Night targets are watched like state entities (unless `state_entities` is
  set explicitly), so a manually-lit night light blocks activation as usual.
- A missing/unavailable mode entity **fails open to day behaviour** — a broken
  helper skips the night tweaks, it never dims the house.
- `mode` (day/night) and `active_entities` attributes expose the current
  choice for diagnostics.

## State Persistence

EC persists its state across Home Assistant restarts using the built-in HA storage layer: `overridden`, `blocked` and a running timer (`active_timer`, with the time it was due to run out). The state is saved when Home Assistant stops and whenever EC enters or leaves `overridden` or `blocked`. On startup it is checked against the live entity states before it is applied: if an override entity is on, EC starts in `overridden`; if every state entity is off, it starts in `idle`, as on a normal start.

- A restored `blocked` state leaves the lights as they are and re-arms the block timer.
- A restored timer never switches the lights off at startup while a sensor or hold sensor is on or has not reported yet (Zigbee2MQTT sensors are often still `unknown` then). It resumes with the time that was left, but at least the configured `delay` while a sensor has not reported, and with the `delay` (the night delay at night) when it had run out by the restart. Only a timer that ran out with every sensor reporting off switches the lights off, once. The resumed time never exceeds the longest timer the instance can run (the day or night `delay`, `backoff_max` with `backoff: true`): the saved time is local wall-clock time, which a daylight-saving change or a clock step would otherwise stretch.
- A light that is still `unavailable` or `unknown` does not stop the timer while another light is on. When one returns from `unavailable`, the usual comeback resync applies (one that returns off is switched on while the timer runs); one that was still `unknown` is treated as described under Known limits. When no reachable light is on and some have not reported yet, EC waits in `pending` for up to 5 minutes: the first light that returns on resumes the timer as above, lights that all return off end in `idle` without a turn-off once they have stayed off for `state_change_debounce` (a Tasmota light reports a placeholder `off` 1–2 s before its real state, which then settles the timer as above), and after 5 minutes EC starts in `idle` without switching anything off. When the wait ends in `idle` and a regular (non-hold) sensor is on, EC acts on it as on a new motion edge (the edge itself is not seen during the wait). A light switched on during the wait (by hand or an automation) is not switched off by the restore: the wait then ends in `blocked`, also at the 5-minute bound while that light is still on, as for any light switched on outside EC while `idle` (see Block on External ON; with `block_on_external_on: false` or `disable_block: true` it ends in `idle`). This does not hold for a turn-on within `state_change_debounce` of that light's own return, which counts as its real state as above, nor across a stop of Home Assistant during the wait: the next start treats the light as EC's. An override that turns on, the end of the start/end window (constrain) or a forced sensor ends the wait.

Known limits: after a crash (no clean stop) EC restores the last saved state, which can be old; the resumed time is then bounded as above. A light that is still `unknown` when the timer resumes is treated as an external change when it first reports, so EC may go `blocked`. A light that is still `unknown` when the 5-minute wait ends, and then reports on, counts as switched on outside EC: EC goes `blocked`, and the light goes off when the block ends in an empty room (with `block_on_external_on: false` it stays on until the next sensor activity). Motion during the wait lights the room only when the wait ends.

No configuration is required — persistence is enabled automatically.

## Block Timer Fix

Prior to v9.8.0, when the block timer expired while all state entities were already off, the controller was left stuck in the `blocked` state (issue #310). This has been fixed: the state machine now correctly transitions `blocked → idle` when the block timer expires and all state entities are off.

## Grace Period (`grace_period`)

**Problem:** Cloud and gateway integrations — such as [Tahoma / Somfy](https://www.home-assistant.io/integrations/tahoma/) — update entity states asynchronously by polling the cloud. When EC calls `light.turn_on`, the integration sends the command upstream and only confirms the new state seconds later through its next poll cycle. That delayed state-change event is emitted with a fresh HA context that has no relationship to the original EC service call. Because EC's self-suppression mechanism (`is_ignored_context`) looks for its own context id prefix, it misses the late-arriving event, fires `control()`, finds the state entity on, and transitions `active_timer → blocked`.

**Solution:** Set `grace_period` to a value (in seconds) that covers the integration's worst-case round-trip latency. EC will then ignore all state-entity changes that arrive within that window after any service call, preventing false `blocked` transitions (in `idle`, an ON edge is judged after the grace period ends instead; see Block on External ON).

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
| `grace_period` | integer (seconds) | `null` (disabled) | Duration after each service call during which state-entity changes are ignored (in `idle`, an ON edge is judged after it ends instead; see Block on External ON). Set to a value slightly above your integration's worst-case latency. |

**When to use it:** Only needed for integrations where state-change events arrive with a context unrelated to the original EC service call (cloud integrations, gateway bridges, etc.). Standard local integrations — where HA propagates the service-call context through to the state-change event — are handled correctly by the existing context check and do not need this option.

## State-Change Debounce (`state_change_debounce`)

**Problem:** Some devices report a short `on -> off -> on` bounce (a relay re-reporting, a Wi-Fi blip) with a context that is not EC's own. EC read the first edge as manual control. In `active_timer` it went to `blocked`, or to `idle` (switching the lights off) if nothing else was on, or restarted the timer under `disable_block`. In `blocked`/`active_stay_on` the bounce of the last lit entity sent EC to `idle`, switching the light off.

**Solution:** After an external change of a state entity in `active_timer`, `blocked` or `active_stay_on`, and in `idle` for a light switched on (see Block on External ON), EC waits `state_change_debounce` seconds. It then compares every entity that changed with its state before the first change. If all of them are back, the change is ignored; otherwise EC reacts once, as before. The window starts at the first change, and later changes do not extend it.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `state_change_debounce` | number (seconds, 0-60) | `3` | How long an external change must persist before EC reacts. `0` or `null` reacts immediately (behaviour before 9.11.3; in `idle`, an ON edge blocks at once). A boolean, a non-number, NaN or a value outside 0-60 logs an error and uses 3. |

- Changes that EC already ignores never open the window: its own context or an `ignored_event_sources` match, `grace_period` (in `idle`, an ON edge is judged after the grace period ends instead), a drop to or return from `unavailable` (a return is resynced instead), and attributes listed in `state_attributes_ignore`. In `idle`, anything but an ON edge opens no window; once one is open, every change outside the grace period joins it (inside it, only an ON edge does).
- When only attributes changed, attributes are compared. Once the state itself changed, only the state is.
- An entity that is `unavailable` at the end of the window is judged by the last state it reported in the window. A removed entity is skipped.
- The check is dropped if EC left the state it was opened in, entered `idle`, or is in another state at the deadline. A window opened in `blocked` survives a motion onset that re-enters `blocked` (intended), a hold-sensor restart of the block timer, and a `blocked` -> `constrained` -> `blocked` round trip inside the window (a known gap). The check is also dropped if `grace_period` is set and that grace period is still running at the deadline, which means EC called a service in the meantime.

Known limits (set `0` on an instance where one of them matters; `0` also removes the settle window of a restored timer's wait, so a light that returns with a placeholder `off` then ends that wait, see State Persistence):

- A change made less than `state_change_debounce` seconds before the timer runs out is not seen; the timer turns the lights off.
- A second bounce that straddles the end of the window still counts as a change.
- An entity may return from `unavailable` inside the window while the timer runs. EC's resync then turns the lights back on, and the change is usually lost.
- A Home Assistant restart inside the window drops the pending check.
- After a real external off, a motion onset or `entity_controller.activate` that arrives inside the window only restarts the timer; at the deadline EC goes to `idle` and the light stays off until the next sensor onset (before 9.11.3 it was re-lit at once).

## Block on External ON (`block_on_external_on`)

**Problem:** A light switched on outside EC (a wall switch, the app, another automation) while EC was `idle` was left alone. EC noticed it only at the next motion onset, which put EC in `blocked`; when nobody came, the light stayed on until someone switched it off.

**Solution:** EC takes such a light over. When a state entity goes from `off`, `unknown` or no state to `on` with a context that is not EC's own, and is still on after `state_change_debounce`, EC goes from `idle` to `blocked` and starts the `block_timeout` timer. Without `block_timeout`, the block lasts until the lights are switched off, as any block. The take-over itself makes no service call. When the block ends with nobody in the room, EC switches the light off.

```yaml
entity_controller:
  bedroom:
    sensor: binary_sensor.bedroom_motion
    entity: light.bedroom_reading
    block_timeout: 1800
    block_on_external_on: false   # leave a lamp switched on by hand alone
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `block_on_external_on` | boolean | `true` | Take over a light switched on outside EC while EC is `idle` (go to `blocked`). Only a YAML `true`/`false` is accepted; anything else (a quoted `"false"`, `0`, `null`) logs an error and uses `true`. The value is shown as the `block_on_external_on` attribute; `false` is also logged at startup (INFO). |

What counts:

- The ON edge is judged like any external change (see State-Change Debounce): the light must still be on when the window ends, so `off -> on -> off` inside the window changes nothing. With `state_change_debounce` `0`/`null`, the ON edge blocks at once.
- An ON edge inside `grace_period` (EC's own echo in `idle` is an off) is not dropped: it is judged `state_change_debounce` seconds (at least 1 s) after the grace period ends, unless EC called a service in the meantime.
- It does not count: EC's own context or an `ignored_event_sources` match, a return from `unavailable` (the comeback resync applies: off when the room is quiet, adopted when a sensor is on), an attribute-only change, an off edge, any change outside `idle` (handled as before), and anything with `block_on_external_on: false` or `disable_block: true`.
- The take-over logs `<light> switched on outside EC while idle; blocked` (INFO) and sets `last_triggered_by` and `notes` ("Switched on outside EC while idle"); `blocked_by` names the first state entity that is on. An ON edge that does not persist logs at DEBUG only.

How the block ends:

- At the block timer's expiry the usual rules apply. With every sensor off (a hold sensor that has been on for `hold_max_seconds` counts as off), EC goes to `idle`, and the `on_enter_idle` turn-off (the default) switches off every controlled light that is on: the day and the night targets (`entity`/`entities` and `night_mode: entities`), not only the set of the last activation. An entity listed only in `state_entities` is never switched off.
- With a sensor or hold sensor on, EC goes to `active`: it switches on every control entity of the current mode with that mode's `service_data` (a brightness set by hand is overwritten) and runs the timer as usual. With `sensor_type: event`, EC goes to `active` whenever a light is still on.
- Switching every light off ends the block in `idle`, with the same turn-off as at the expiry. An override, `constrain` or a forced sensor ends it as before.

Hold sensors keep any block alive (also with `block_on_external_on: false`): a hold sensor that turns on in `blocked` restarts the block timer with `block_timeout`, at most once a minute, and only within `hold_max_seconds` of the block start or the last regular-sensor onset (which re-enters `blocked`, as before). The restart neither leaves nor re-enters `blocked`, saves nothing and switches nothing; `expires_at` and `notes` show it.

After a restart, a light switched on while a restored timer waits for its lights ends the wait in `blocked` too (see State Persistence).

Known limits:

- A light that is `unavailable` when the window ends, and was the only one on, leaves EC in `idle`; its comeback is then resynced (off when the room is quiet, adopted when a sensor is on).
- An ON edge inside the grace period that joins an older window keeps that window's deadline, and is dropped if that deadline falls inside the grace period (seen only after a comeback resync switched the lights off). A wall-clock step back inside a grace period (the daylight-saving fall-back) makes the grace period last up to an hour longer, as before; an ON edge in that time is dropped, logged as if EC had called a service meanwhile.
- A light returning `unavailable -> unknown -> on` counts as switched on, unlike `unavailable -> on`. So does a relay that reboots to `off` and then reports `on`.
- An automation that EC triggered counts as external.
- A light left on deliberately in an empty room goes off when the block ends; set `block_on_external_on: false` on such an instance.
- A block that ends by adoption (a sensor on at the expiry) hands the lights to the current mode's set: when that timer runs out, a lamp of the other mode switched on by hand stays on (only with `night_mode: entities`).
- A restart restarts `block_timeout` and the `hold_max_seconds` bound for hold sensors, and keeps the block only if a light reports on when the state is restored. A relay that is still `unavailable` then falls back to the normal start and its comeback resync.
- A block ended by switching the light off still sends the usual `on_enter_idle` turn-off.
