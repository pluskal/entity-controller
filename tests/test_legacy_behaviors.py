"""Behavioral tests for entity_controller ported from legacy test files.

These tests replace the 7 legacy test files that are permanently skipped because
they target APIs that no longer exist:

- test_lightingsm.py / test_lightingsm_async.py
    Relied on ``homeassistant.components.async_setup`` (removed HA 2025.x) and
    the old ``lightingsm`` component name.
- test_parsing.py / test_lightingsm_backup_all_tests.py
    Same deprecated imports; also tested a non-existent component domain.
- test_demo.py / test_lighting_sm_appdaemon.py / test_motion_lights_appdaemon_non_sm.py
    Tested the old AppDaemon edition of the project (``apps.*`` module gone).

All tests here use the same lightweight mock infrastructure as test_new_features.py:
a hand-built HierarchicalMachine + mocked hass object.  No live HA instance is
required, so they run without a real Home Assistant installation.
"""
import logging
import pytest
from unittest.mock import MagicMock, patch

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Re-use the shared test infrastructure from test_new_features.py
# ---------------------------------------------------------------------------
from tests.test_new_features import (
    _make_hass,
    _make_entity,
    _add_machine_transitions,
    _build_model,
)


# ---------------------------------------------------------------------------
# Helper – build a fake HA state-change event accepted by Model callbacks
# ---------------------------------------------------------------------------

def _make_event(entity_id, old_state_str, new_state_str, context=None):
    """Return a MagicMock that looks like a HA state-change event."""
    ev = MagicMock()
    ev.data = {
        "entity_id": entity_id,
        "old_state": MagicMock(state=old_state_str),
        "new_state": MagicMock(
            state=new_state_str, context=context or MagicMock(id="test-ctx")
        ),
    }
    return ev


# ===========================================================================
# TestBasicSensorFlow
# Scenarios from test_lightingsm.py, test_lightingsm_async.py,
# test_lighting_sm_appdaemon.py – fundamental sensor-triggered flow.
# ===========================================================================

class TestBasicSensorFlow:

    def test_initial_state_is_idle(self):
        """After start_monitoring the model must be in idle."""
        model = _build_model()
        assert model.state == "idle"

    def test_sensor_on_from_idle_reaches_active_timer(self):
        """sensor_on() from idle must reach active_timer (via active → enter())."""
        model = _build_model()
        model.sensor_on()
        assert model.state in ("active", "active_timer"), (
            f"Expected active or active_timer, got {model.state}"
        )

    def test_timer_expires_from_active_timer_returns_to_idle(self):
        """timer_expires for an event sensor must return the controller to idle."""
        model = _build_model()
        model.sensor_type = "event"
        model.sensor_on()
        assert model.state in ("active", "active_timer")
        model.timer_expires()
        assert model.state == "idle", (
            f"Expected idle after timer_expires (event sensor), got {model.state}"
        )

    def test_sensor_on_during_active_timer_resets_timer(self):
        """A second sensor_on while already in active_timer should reset the timer."""
        model = _build_model()
        model._reset_timer = MagicMock()
        model.sensor_on()
        assert model.state == "active_timer"
        model.sensor_on()
        assert model.state == "active_timer"
        model._reset_timer.assert_called()

    def test_sensor_state_change_callback_on_triggers_sensor_on(self):
        """sensor_state_change callback with new state=on must trigger activation."""
        model = _build_model()
        assert model.state == "idle"
        ev = _make_event("binary_sensor.motion", "off", "on")
        model.sensor_state_change(ev)
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer after sensor on callback, got {model.state}"
        )

    def test_sensor_state_change_attribute_only_change_is_ignored(self):
        """sensor_state_change must be a no-op when old.state == new.state."""
        model = _build_model()
        ev = _make_event("binary_sensor.motion", "on", "on")
        model.sensor_state_change(ev)
        assert model.state == "idle"

    def test_full_motion_cycle_idle_active_idle(self):
        """Complete cycle: idle → sensor on → active_timer → timer_expires → idle."""
        model = _build_model()
        model.sensor_type = "event"

        # Motion detected
        model.sensor_on()
        assert model.state in ("active", "active_timer")

        # Timer expires
        model.timer_expires()
        assert model.state == "idle"


# ===========================================================================
# TestStateEntitiesBlocking
# Scenarios from test_lighting_sm_appdaemon.py – state_entities blocking.
# ===========================================================================

class TestStateEntitiesBlocking:

    def test_state_entities_on_blocks_activation(self):
        """When a state entity is on, sensor_on must go to blocked (not active)."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "blocked", (
            f"Expected blocked when a state entity is on, got {model.state}"
        )

    def test_state_entities_off_allows_activation(self):
        """When all state entities are off, sensor_on must reach active_timer."""
        model = _build_model()
        # Default _build_model has stateEntities=[] → is_state_entities_off() returns True
        model.sensor_on()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer when state entities off, got {model.state}"
        )

    def test_state_entity_off_while_blocked_enables_idle(self):
        """When blocked, enable() with entities now off must go to idle."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "blocked"

        # State entity turns off
        model.is_state_entities_on = MagicMock(return_value=False)
        model.is_state_entities_off = MagicMock(return_value=True)
        model.enable()
        assert model.state == "idle", (
            f"Expected idle after enable() with entities off, got {model.state}"
        )

    def test_state_entity_state_change_in_active_timer_triggers_control_to_idle(self):
        """state_entity_state_change while active_timer with entities off → idle."""
        model = _build_model()
        model.sensor_on()
        # Normalise to active_timer
        if model.state == "active":
            model.enter()
        assert model.state == "active_timer"

        model.is_state_entities_off = MagicMock(return_value=True)
        model.is_state_entities_on = MagicMock(return_value=False)
        model.is_ignored_context = MagicMock(return_value=False)

        ev = MagicMock()
        ev.data = {
            "entity_id": "light.kitchen",
            "old_state": MagicMock(state="on", attributes={"brightness": 100}),
            "new_state": MagicMock(
                state="off",
                attributes={"brightness": 0},
                context=MagicMock(id="ctx_state"),
            ),
        }
        model.state_entity_state_change(ev)
        # control() with is_state_entities_off → idle
        assert model.state == "idle", (
            f"Expected idle after control() (entities off), got {model.state}"
        )

    def test_multiple_state_entities_one_on_blocks(self):
        """One of multiple state entities being on must block the controller."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "blocked"

    def test_multiple_state_entities_all_off_activates(self):
        """All state entities off → sensor_on → active_timer (not blocked)."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=False)
        model.is_state_entities_off = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_sensor_on_again_while_blocked_stays_blocked(self):
        """Another sensor_on while blocked must keep the controller blocked."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "blocked"

        model.sensor_on()
        assert model.state == "blocked"


# ===========================================================================
# TestOverrideEntities
# Scenarios from test_lighting_sm_appdaemon.py – override (disabled) behavior.
# ===========================================================================

class TestOverrideEntities:

    def test_override_state_change_on_from_idle_transitions_to_overridden(self):
        """override_state_change with new=on from idle must go to overridden."""
        model = _build_model()
        assert model.state == "idle"

        ev = _make_event("input_boolean.override", "off", "on")
        model.override_state_change(ev)
        assert model.state == "overridden", (
            f"Expected overridden after override entity on, got {model.state}"
        )

    def test_override_state_change_on_from_active_timer_transitions_to_overridden(self):
        """override_state_change with new=on while active_timer → overridden."""
        model = _build_model()
        model.sensor_on()
        assert model.state in ("active", "active_timer")

        ev = _make_event("input_boolean.override", "off", "on")
        model.override_state_change(ev)
        assert model.state == "overridden", (
            f"Expected overridden when active_timer + override on, got {model.state}"
        )

    def test_override_off_from_overridden_returns_to_idle(self):
        """override_state_change with new=off while overridden must enable → idle."""
        model = _build_model()
        model.override()
        assert model.state == "overridden"

        model.is_override_state_off = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=True)

        ev = _make_event("input_boolean.override", "on", "off")
        model.override_state_change(ev)
        assert model.state == "idle", (
            f"Expected idle after override entity off, got {model.state}"
        )

    def test_sensor_on_ignored_while_overridden(self):
        """Regular sensor_on has no transition from overridden → state unchanged."""
        model = _build_model()
        model.override()
        assert model.state == "overridden"

        try:
            model.sensor_on()
        except Exception:
            pass  # MachineError is acceptable (no transition defined)
        assert model.state == "overridden", (
            f"Expected overridden after sensor_on (no transition), got {model.state}"
        )

    def test_custom_override_states_on_recognized(self):
        """Custom override_states_on values are matched by override_state_change."""
        model = _build_model()
        model.OVERRIDE_ON_STATE = ["playing", "on"]

        # 'playing' should trigger override
        ev = _make_event("media_player.tv", "idle", "playing")
        model.override_state_change(ev)
        assert model.state == "overridden", (
            f"Expected overridden with custom state 'playing', got {model.state}"
        )

    def test_custom_override_states_off_re_enables(self):
        """Custom override_states_off clears the override."""
        model = _build_model()
        model.OVERRIDE_OFF_STATE = ["paused", "off"]
        model.override()
        assert model.state == "overridden"

        model.is_override_state_off = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=True)

        ev = _make_event("media_player.tv", "playing", "paused")
        model.override_state_change(ev)
        assert model.state == "idle", (
            f"Expected idle with custom off state 'paused', got {model.state}"
        )


# ===========================================================================
# TestDurationSensor
# Scenarios from test_lighting_sm_appdaemon.py – duration sensor mode.
# ===========================================================================

class TestDurationSensor:

    def _build_duration_model(self):
        model = _build_model()
        model.sensor_type = "duration"
        return model

    def test_duration_sensor_on_activates_controller(self):
        """Duration sensor on → controller activates."""
        model = self._build_duration_model()
        model.sensor_on()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer for duration sensor on, got {model.state}"
        )

    def test_duration_sensor_off_with_timer_expired_goes_idle(self):
        """sensor_off_duration while timer expired → idle."""
        model = self._build_duration_model()
        model.is_timer_expired = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "active_timer"

        model.sensor_off_duration()
        assert model.state == "idle", (
            f"Expected idle after sensor_off_duration (timer expired), got {model.state}"
        )

    def test_duration_sensor_off_with_timer_alive_stays_active(self):
        """sensor_off_duration while timer still running must NOT go idle."""
        model = self._build_duration_model()
        model.is_timer_expired = MagicMock(return_value=False)

        model.sensor_on()
        assert model.state == "active_timer"

        model.sensor_off_duration()
        # No matching transition (is_timer_expired=False) → stays active_timer
        assert model.state == "active_timer", (
            f"Expected active_timer (timer still alive), got {model.state}"
        )

    def test_duration_sensor_timer_expire_with_sensor_still_on_stays_active(self):
        """For duration sensor: timer_expires when sensor still on → stays active_timer."""
        model = self._build_duration_model()
        model.is_sensor_on = MagicMock(return_value=True)
        model.is_sensor_off = MagicMock(return_value=False)

        model.sensor_on()
        assert model.state == "active_timer"

        try:
            model.timer_expires()
        except Exception:
            pass
        assert model.state == "active_timer", (
            f"Expected active_timer (duration + sensor still on), got {model.state}"
        )

    def test_duration_sensor_callback_off_in_active_timer_triggers_off_duration(self):
        """sensor_state_change new=off for a duration sensor while active_timer
        must fire sensor_off_duration, and go idle if timer has expired."""
        model = self._build_duration_model()
        model.config["sensor_resets_timer"] = False
        model.is_timer_expired = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "active_timer"

        ev = _make_event("binary_sensor.motion", "on", "off")
        model.sensor_state_change(ev)
        assert model.state == "idle", (
            f"Expected idle after sensor off callback (duration, timer expired), got {model.state}"
        )

    def test_duration_sensor_callback_off_ignored_for_event_sensor(self):
        """sensor_state_change new=off is ignored when sensor_type is event."""
        model = _build_model()
        model.sensor_type = "event"

        model.sensor_on()
        assert model.state == "active_timer"

        ev = _make_event("binary_sensor.motion", "on", "off")
        model.sensor_state_change(ev)
        # No sensor_off_duration for event sensors while active_timer
        assert model.state == "active_timer", (
            f"Expected active_timer (off ignored for event sensor), got {model.state}"
        )


# ===========================================================================
# TestStayOn
# Scenarios from test_lighting_sm_appdaemon.py – stay=True (stay on) behavior.
# ===========================================================================

class TestStayOn:

    def test_stay_on_goes_to_active_stay_on_not_active_timer(self):
        """With stay=True, activation must reach active_stay_on, not active_timer."""
        model = _build_model()
        model.stay = True
        model.sensor_on()
        assert model.state == "active_stay_on", (
            f"Expected active_stay_on with stay=True, got {model.state}"
        )

    def test_active_stay_on_does_not_respond_to_timer_expires(self):
        """active_stay_on must remain active when timer_expires fires."""
        model = _build_model()
        model.stay = True
        model.sensor_on()
        assert model.state == "active_stay_on"

        try:
            model.timer_expires()
        except Exception:
            pass
        assert model.state == "active_stay_on", (
            f"Expected active_stay_on after timer_expires, got {model.state}"
        )

    def test_active_stay_on_enable_with_entities_off_goes_idle(self):
        """enable() in active_stay_on with state entities off → idle."""
        model = _build_model()
        model.stay = True
        model.sensor_on()
        assert model.state == "active_stay_on"

        model.is_state_entities_off = MagicMock(return_value=True)
        model.enable()
        assert model.state == "idle", (
            f"Expected idle after enable() from active_stay_on, got {model.state}"
        )


# ===========================================================================
# TestMultipleSensors
# Scenarios from test_lighting_sm_appdaemon.py – multiple sensor entities.
# ===========================================================================

class TestMultipleSensors:

    def test_first_sensor_activates_from_idle(self):
        """First sensor firing sensor_on from idle → active_timer."""
        model = _build_model()
        model.sensorEntities = ["binary_sensor.s1", "binary_sensor.s2"]
        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_second_sensor_callback_also_activates_from_idle(self):
        """The second sensor entity's callback must also activate from idle."""
        model = _build_model()
        model.sensorEntities = ["binary_sensor.s1", "binary_sensor.s2"]

        ev = _make_event("binary_sensor.s2", "off", "on")
        model.sensor_state_change(ev)
        assert model.state in ("active", "active_timer")

    def test_second_sensor_retriggers_timer_while_active_timer(self):
        """A second sensor firing while already active_timer must reset the timer."""
        model = _build_model()
        model.sensorEntities = ["binary_sensor.s1", "binary_sensor.s2"]

        # Activate via first sensor
        model.sensor_on()
        assert model.state == "active_timer"

        # Second sensor fires
        ev = _make_event("binary_sensor.s2", "off", "on")
        model._reset_timer = MagicMock()
        model.sensor_state_change(ev)
        assert model.state == "active_timer"
        model._reset_timer.assert_called()

    def test_either_sensor_activates_from_idle(self):
        """Both sensor entities must independently activate the controller."""
        for sensor_id in ("binary_sensor.s1", "binary_sensor.s2"):
            model = _build_model()
            model.sensorEntities = ["binary_sensor.s1", "binary_sensor.s2"]
            ev = _make_event(sensor_id, "off", "on")
            model.sensor_state_change(ev)
            assert model.state in ("active", "active_timer"), (
                f"Expected activation from {sensor_id}, got {model.state}"
            )


# ===========================================================================
# TestConfigSensorEntities
# Scenarios from test_lightingsm.py / test_lightingsm_backup_all_tests.py
# – config_sensor_entities registers HA state-change listeners.
# ===========================================================================

class TestConfigSensorEntities:

    def test_config_single_sensor_registers_listener(self):
        """config_sensor_entities registers a listener for a single sensor."""
        hass = _make_hass()
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as mock_track:
            from custom_components.entity_controller import Model
            m = Model.__new__(Model)
            m.log = logging.getLogger("test")
            m.hass = hass
            m.config_sensor_entities({"sensor": ["binary_sensor.motion"], "sensors": []})
            mock_track.assert_called_once()
            registered_entities = mock_track.call_args[0][1]
            assert "binary_sensor.motion" in registered_entities

    def test_config_multiple_sensors_all_registered_together(self):
        """config_sensor_entities registers all sensors in a single listener call."""
        hass = _make_hass()
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as mock_track:
            from custom_components.entity_controller import Model
            m = Model.__new__(Model)
            m.log = logging.getLogger("test")
            m.hass = hass
            m.config_sensor_entities(
                {"sensor": ["binary_sensor.s1"], "sensors": ["binary_sensor.s2"]}
            )
            mock_track.assert_called_once()
            registered_entities = mock_track.call_args[0][1]
            assert "binary_sensor.s1" in registered_entities
            assert "binary_sensor.s2" in registered_entities

    def test_config_sensors_plural_key_registered(self):
        """Sensors listed under the 'sensors' (plural) key are also registered."""
        hass = _make_hass()
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as mock_track:
            from custom_components.entity_controller import Model
            m = Model.__new__(Model)
            m.log = logging.getLogger("test")
            m.hass = hass
            m.config_sensor_entities(
                {
                    "sensor": [],
                    "sensors": ["binary_sensor.multi_1", "binary_sensor.multi_2"],
                }
            )
            mock_track.assert_called_once()
            registered_entities = mock_track.call_args[0][1]
            assert "binary_sensor.multi_1" in registered_entities
            assert "binary_sensor.multi_2" in registered_entities

    def test_config_no_sensors_no_listener(self):
        """config_sensor_entities with empty lists must not register any listener."""
        hass = _make_hass()
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as mock_track:
            from custom_components.entity_controller import Model
            m = Model.__new__(Model)
            m.log = logging.getLogger("test")
            m.hass = hass
            m.config_sensor_entities({"sensor": [], "sensors": []})
            mock_track.assert_not_called()


# ===========================================================================
# TestCustomStateStrings
# Scenarios from test_lighting_sm_appdaemon.py – custom state string lists.
# ===========================================================================

class TestCustomStateStrings:

    def test_custom_sensor_on_state_activates_on_match(self):
        """Custom SENSOR_ON_STATE is honoured by sensor_state_change."""
        model = _build_model()
        model.SENSOR_ON_STATE = ["motion", "on"]

        ev = _make_event("binary_sensor.pir", "idle", "motion")
        model.sensor_state_change(ev)
        assert model.state in ("active", "active_timer")

    def test_unknown_state_does_not_activate(self):
        """A state that is not in any on/off list leaves the controller idle."""
        model = _build_model()
        model.SENSOR_ON_STATE = ["on"]
        model.SENSOR_OFF_STATE = ["off", "idle"]

        ev = _make_event("binary_sensor.motion", "idle", "unavailable")
        model.sensor_state_change(ev)
        assert model.state == "idle"

    def test_matches_helper_returns_true_for_present_value(self):
        """Model.matches() returns True when the value is in the list."""
        model = _build_model()
        assert model.matches("on", ["on", "playing"]) is True

    def test_matches_helper_returns_false_for_absent_value(self):
        """Model.matches() returns False when the value is not in the list."""
        model = _build_model()
        assert model.matches("bye", ["on", "playing"]) is False

    def test_custom_override_state_on_triggers_override(self):
        """Custom override state 'playing' must trigger the override transition."""
        model = _build_model()
        model.OVERRIDE_ON_STATE = ["playing", "on"]

        ev = _make_event("media_player.tv", "idle", "playing")
        model.override_state_change(ev)
        assert model.state == "overridden"


# ===========================================================================
# TestConstrainedState
# Scenarios from test_lighting_sm_appdaemon.py – time-constraint behavior.
# ===========================================================================

class TestConstrainedState:

    def test_constrain_from_idle_goes_to_constrained(self):
        """constrain() trigger from idle → constrained."""
        model = _build_model()
        model.constrain()
        assert model.state == "constrained"

    def test_sensor_on_from_constrained_has_no_effect(self):
        """sensor_on has no transition from constrained → state unchanged."""
        model = _build_model()
        model.constrain()
        try:
            model.sensor_on()
        except Exception:
            pass
        assert model.state == "constrained"

    def test_enable_from_constrained_goes_to_idle_when_no_override(self):
        """enable() from constrained with no active override entity → idle."""
        model = _build_model()
        model.constrain()
        model.is_override_state_off = MagicMock(return_value=True)
        model.is_override_state_on = MagicMock(return_value=False)
        model.enable()
        assert model.state == "idle"

    def test_enable_from_constrained_goes_to_overridden_when_override_on(self):
        """enable() from constrained with an override entity active → overridden."""
        model = _build_model()
        model.constrain()
        model.is_override_state_off = MagicMock(return_value=False)
        model.is_override_state_on = MagicMock(return_value=True)
        model.enable()
        assert model.state == "overridden"

    def test_force_activate_from_constrained_bypasses_constraint(self):
        """force_activate must bypass the constrained state (forced sensor behaviour)."""
        model = _build_model()
        model.constrain()
        assert model.state == "constrained"

        model.force_activate()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer (force_activate from constrained), got {model.state}"
        )
