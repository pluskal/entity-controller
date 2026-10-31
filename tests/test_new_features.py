"""Tests for fork-integration features added to entity_controller.

Tests cover:
  Phase 1 – block_timer_expires → idle when state entities are already off
  Phase 3 – forced_sensors bypass blocked/constrained/overridden
  Phase 6 – HA bus event sensors trigger activation
  Phase 2 – state persistence helper methods
"""
import asyncio
import logging
import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Minimal helpers to exercise the Model state machine without a full HA setup
# ---------------------------------------------------------------------------

def _make_hass():
    hass = MagicMock()
    hass.loop = asyncio.new_event_loop()
    hass.states = MagicMock()
    hass.services = MagicMock()
    hass.services.async_call = AsyncMock()
    hass.bus = MagicMock()
    hass.bus.async_listen = MagicMock(return_value=lambda: None)
    hass.bus.async_listen_once = MagicMock(return_value=lambda: None)
    def _safe_create_task(coro):
        if asyncio.iscoroutine(coro):
            coro.close()
    hass.async_create_task = MagicMock(side_effect=_safe_create_task)
    return hass


def _make_entity():
    entity = MagicMock()
    entity.set_attr = MagicMock()
    entity.do_update = MagicMock()
    entity.reset_state = MagicMock()
    entity.async_set_context = MagicMock()
    return entity


def _add_machine_transitions(machine):
    machine.add_transition(trigger="start_monitoring", source="pending", dest="idle")
    machine.add_transition(trigger="restore_blocked", source="pending", dest="blocked")
    machine.add_transition(trigger="restore_active_timer", source="pending", dest="active")
    machine.add_transition(trigger="constrain", source="*", dest="constrained")
    machine.add_transition(
        trigger="override",
        source=["pending", "idle", "active_timer", "blocked"],
        dest="overridden",
    )
    machine.add_transition(trigger="activate", source=["idle", "blocked"], dest="active")
    machine.add_transition(
        trigger="activate", source="active_timer", dest=None, after="_reset_timer"
    )
    machine.add_transition(
        trigger="sensor_on", source="idle", dest="active",
        conditions=["is_state_entities_off", "is_lux_constraint_satisfied"],
    )
    machine.add_transition(
        trigger="sensor_on", source="idle", dest="active",
        conditions=["is_state_entities_on"],
        unless="is_block_enabled",
    )
    machine.add_transition(
        trigger="sensor_on", source="idle", dest="blocked",
        conditions=["is_state_entities_on", "is_block_enabled"],
    )
    machine.add_transition(
        trigger="enable", source="idle", dest=None,
        conditions=["is_state_entities_off"],
    )
    machine.add_transition(
        trigger="enable", source="blocked", dest="idle",
        conditions=["is_state_entities_off"],
    )
    machine.add_transition(
        trigger="sensor_on", source="blocked", dest="blocked",
        conditions=["is_block_enabled"],
    )
    machine.add_transition(
        trigger="enable", source="overridden", dest="idle",
        conditions=["is_state_entities_off"],
    )
    machine.add_transition(
        trigger="enable", source="overridden", dest="active",
        conditions=["is_state_entities_on", "is_event_sensor"],
    )
    machine.add_transition(
        trigger="enable", source="overridden", dest="active",
        conditions=["is_state_entities_on", "is_sensor_on"],
    )
    machine.add_transition(
        trigger="enable", source="overridden", dest="idle",
        conditions=["is_state_entities_on", "is_duration_sensor", "is_sensor_off"],
    )
    machine.add_transition(
        trigger="enter", source="active", dest="active_timer", unless="will_stay_on",
    )
    machine.add_transition(
        trigger="enter", source="active", dest="active_stay_on", conditions="will_stay_on",
    )
    machine.add_transition(
        trigger="sensor_on", source="active_timer", dest=None, after="_reset_timer",
    )
    machine.add_transition(
        trigger="sensor_off_duration", source="active_timer", dest="idle",
        conditions=["is_timer_expired"],
    )
    machine.add_transition(
        trigger="timer_expires", source="active_timer", dest="idle",
        conditions=["is_event_sensor"],
    )
    machine.add_transition(
        trigger="timer_expires", source="active_timer", dest="idle",
        conditions=["is_duration_sensor", "is_sensor_off"],
    )
    # Phase 1 fix: block_timer_expires → idle when entities already off
    machine.add_transition(
        trigger="block_timer_expires", source="blocked", dest="idle",
        conditions=["is_state_entities_off"],
    )
    machine.add_transition(
        trigger="block_timer_expires", source="blocked", dest="active",
        conditions=["is_state_entities_on", "is_event_sensor"],
    )
    machine.add_transition(
        trigger="block_timer_expires", source="blocked", dest="active",
        conditions=["is_state_entities_on", "is_sensor_on"],
    )
    machine.add_transition(
        trigger="block_timer_expires", source="blocked", dest="idle",
        conditions=["is_state_entities_on", "is_duration_sensor", "is_sensor_off"],
    )
    # Phase 2 fix: catch-all — covers sensor_off + state_entities_on (slow
    # cloud integrations like Overkiz that report stale "on" state after
    # block_timeout fires, while the trigger sensor is already off).
    machine.add_transition(
        trigger="block_timer_expires", source="blocked", dest="idle",
    )
    machine.add_transition(
        trigger="control", source="active_timer", dest="idle",
        conditions=["is_state_entities_off"],
    )
    machine.add_transition(
        trigger="control", source="active_timer", dest="blocked",
        conditions=["is_state_entities_on", "is_block_enabled"],
    )
    machine.add_transition(
        trigger="control", source="active_timer", dest=None,
        after="_reset_timer", conditions=["is_state_entities_on"],
        unless="is_block_enabled",
    )
    machine.add_transition(
        trigger="block_enable", source="active_timer", dest="blocked",
        conditions=["is_state_entities_on", "is_block_enabled"],
    )
    machine.add_transition(
        trigger="enable", source="active_stay_on", dest="idle",
        conditions=["is_state_entities_off"],
    )
    machine.add_transition(
        trigger="enable", source="constrained", dest="idle",
        conditions=["is_override_state_off"],
    )
    machine.add_transition(
        trigger="enable", source="constrained", dest="overridden",
        conditions=["is_override_state_on"],
    )
    machine.add_transition(
        trigger="blocked", source="constrained", dest="blocked",
        conditions=["is_block_enabled"],
    )
    # Phase 4: blocked reachable from idle (start_time while already idle)
    machine.add_transition(
        trigger="blocked", source="idle", dest="blocked",
        conditions=["is_block_enabled"],
    )
    # Phase 3: force_activate bypasses all states
    machine.add_transition(
        trigger="force_activate",
        source=["idle", "blocked", "constrained", "overridden", "active_timer"],
        dest="active",
    )


def _build_model(hass=None, entity=None, config=None):
    """Build a fully initialised Model ready for state testing.

    _start_timer and _cancel_timer are patched to no-ops so background
    threading.Timer objects do not keep the process alive after tests finish.
    """
    from transitions.extensions import HierarchicalMachine as Machine
    from custom_components.entity_controller.const import STATES

    machine = Machine(
        states=STATES,
        initial="pending",
        finalize_event="finalize",
    )
    _add_machine_transitions(machine)

    if hass is None:
        hass = _make_hass()
    if entity is None:
        entity = _make_entity()
    if config is None:
        config = {
            "name": "test_ec",
            "entity": "light.test",
            "sensor": "binary_sensor.motion",
        }

    with patch("custom_components.entity_controller.event.async_call_later"):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.ec_startup_time = datetime.now()
        m.hass = hass
        m.entity = entity
        m.config = config
        m.debug_day_length = None
        m.stateEntities = []
        m.controlEntities = []
        m.sensorEntities = ["binary_sensor.motion"]
        m.forcedSensorEntities = []
        m.eventSensorTypes = []
        m._event_sensor_cancel_callbacks = []
        m.triggerOnDeactivate = []
        m.triggerOnActivate = []
        m.overrideEntities = []
        m.timer_handle = None
        m.block_timer_handle = None
        m.sensor_type = "event"
        m.night_mode = None
        m.state_attributes_ignore = []
        m.backoff = False
        m.backoff_count = 0
        m.light_params_day = {"delay": 180, "service_data": None, "service_data_off": None}
        m.light_params_night = {}
        m.lightParams = {"delay": 180, "service_data": None, "service_data_off": None}
        m.name = config.get("name", "test_ec")
        m.stay = False
        m.start = None
        m.end = None
        m.reset_count = None
        m.transition_behaviours = {
            "on_enter_idle": "off",
            "on_exit_idle": "ignore",
            "on_enter_active": "on",
            "on_exit_active": "ignore",
            "on_enter_overridden": "ignore",
            "on_exit_overridden": "ignore",
            "on_enter_constrained": "ignore",
            "on_exit_constrained": "ignore",
            "on_enter_blocked": "ignore",
            "on_exit_blocked": "ignore",
        }
        m.log = logging.getLogger("test.entity_controller.test_ec")
        m.ignored_event_sources = []
        m.context = None
        m._store = None
        m.luxEntity = None
        m.lux_threshold = None
        m.lux_bright_states = []
        m.lux_recheck_delay = 1.0
        m._lux_recheck_handle = None
        m._lux_recheck_entity = None
        m._lux_blocked = False
        m.state_change_debounce = 3.0
        m._debounce_window = None
        m.night_mode_entity = None
        m.night_mode_entity_states = []
        m.nightControlEntities = []
        m.activeControlEntities = []
        m._state_entities_explicit = False
        m.disable_block = False
        m.block_timeout = None
        m.grace_period = None
        m.ignore_state_changes_until = datetime.now()
        m.homeassistant_turn_on_domains = ["group"]

        DEFAULT_ON = ["on", "playing", "home", "True"]
        DEFAULT_OFF = ["off", "idle", "paused", "away", "False"]
        m.CONTROL_ON_STATE = DEFAULT_ON
        m.CONTROL_OFF_STATE = DEFAULT_OFF
        m.SENSOR_ON_STATE = DEFAULT_ON
        m.SENSOR_OFF_STATE = DEFAULT_OFF
        m.OVERRIDE_ON_STATE = DEFAULT_ON
        m.OVERRIDE_OFF_STATE = DEFAULT_OFF
        m.STATE_ON_STATE = DEFAULT_ON
        m.STATE_OFF_STATE = DEFAULT_OFF

        # Patch timer methods so background threads don't block test exit
        m._start_timer = MagicMock()
        m._cancel_timer = MagicMock()

        machine.add_model(m)
        m.start_monitoring()
        return m


# ---------------------------------------------------------------------------
# Phase 1 — block_timer_expires → idle when state entities are off
# ---------------------------------------------------------------------------

class TestBlockTimerExpiresIdleTransition:

    def test_block_timer_expires_to_idle_when_entities_off(self):
        """block_timer_expires must go blocked→idle when the entity is already off.

        This is the Phase 1 fix for issue #310.  Previously no matching
        transition existed for this combination, leaving EC stuck in blocked.
        """
        model = _build_model()

        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "blocked", f"Expected blocked, got {model.state}"

        # Entity turns off while still in blocked
        model.is_state_entities_on = MagicMock(return_value=False)
        model.is_state_entities_off = MagicMock(return_value=True)

        model.block_timer_expires()

        assert model.state == "idle", (
            f"Expected idle after block_timer_expires with entities off, got {model.state}"
        )

    def test_block_timer_expires_to_active_when_entities_on_event_sensor(self):
        """block_timer_expires goes blocked→active when entities ON (event sensor)."""
        model = _build_model()

        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)
        model.is_event_sensor = MagicMock(return_value=True)
        model.is_duration_sensor = MagicMock(return_value=False)

        model.sensor_on()
        assert model.state == "blocked"

        model.block_timer_expires()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer after block_timer_expires entities ON, got {model.state}"
        )

    def test_block_timer_expires_new_transition_present_in_machine(self):
        """The machine must include the block_timer_expires→idle transition for
        the is_state_entities_off condition (Phase 1 fix)."""
        from transitions.extensions import HierarchicalMachine as Machine
        from custom_components.entity_controller.const import STATES

        machine = Machine(states=STATES, initial="pending", finalize_event="finalize")
        _add_machine_transitions(machine)

        # Find transitions triggered by block_timer_expires from the blocked source.
        # The transitions library Condition object stores the function ref in .func
        # which is either a string name or a callable.
        blocked_transitions = [
            t for t in machine.get_transitions("block_timer_expires")
            if t.source == "blocked"
        ]
        def _condition_name(c):
            f = c.func
            return f.__name__ if callable(f) else str(f)

        has_entities_off_transition = any(
            any(_condition_name(c) == "is_state_entities_off" for c in t.conditions)
            for t in blocked_transitions
        )
        assert has_entities_off_transition, (
            "Expected a block_timer_expires→idle transition conditioned on "
            "is_state_entities_off, but it was not found in the machine"
        )

    def test_block_timer_expires_to_idle_when_entities_on_but_sensor_off(self):
        """block_timer_expires must go blocked→idle when state entities are still
        on (e.g. slow cloud/Overkiz feedback) but the trigger sensor is already
        off.

        This is the Phase 2 fix.  Previously no transition matched this
        combination (is_state_entities_on=True, is_sensor_on=False,
        is_duration_sensor=False, is_event_sensor=False), leaving EC stuck in
        blocked past its block_timeout.
        """
        model = _build_model()

        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)
        model.is_sensor_on = MagicMock(return_value=True)
        model.is_sensor_off = MagicMock(return_value=False)
        model.is_event_sensor = MagicMock(return_value=False)
        model.is_duration_sensor = MagicMock(return_value=False)

        model.sensor_on()
        assert model.state == "blocked", f"Expected blocked, got {model.state}"

        # Sensor has gone off, but state entities still report on (slow update)
        model.is_sensor_on = MagicMock(return_value=False)
        model.is_sensor_off = MagicMock(return_value=True)

        model.block_timer_expires()

        assert model.state == "idle", (
            f"Expected idle after block_timer_expires with sensor off + entities "
            f"still on, got {model.state}"
        )

    def test_block_timer_expires_catchall_transition_present_in_machine(self):
        """The machine must include an unconditional block_timer_expires→idle
        catch-all transition (Phase 2 fix)."""
        from transitions.extensions import HierarchicalMachine as Machine
        from custom_components.entity_controller.const import STATES

        machine = Machine(states=STATES, initial="pending", finalize_event="finalize")
        _add_machine_transitions(machine)

        blocked_transitions = [
            t for t in machine.get_transitions("block_timer_expires")
            if t.source == "blocked"
        ]
        # An unconditional transition has an empty conditions list.
        has_catchall = any(len(t.conditions) == 0 for t in blocked_transitions)
        assert has_catchall, (
            "Expected an unconditional block_timer_expires→idle catch-all "
            "transition (Phase 2 fix) but it was not found in the machine"
        )


# ---------------------------------------------------------------------------
# Phase 3 — forced_sensors
# ---------------------------------------------------------------------------

class TestForcedSensors:

    def test_force_activate_from_idle(self):
        model = _build_model()
        assert model.state == "idle"
        model.force_activate()
        assert model.state in ("active", "active_timer")

    def test_force_activate_from_blocked(self):
        """Forced sensor bypasses blocked state."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "blocked"

        model.force_activate()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer from blocked, got {model.state}"
        )

    def test_force_activate_from_constrained(self):
        """Forced sensor bypasses constrained state."""
        model = _build_model()
        model.constrain()
        assert model.state == "constrained"

        model.force_activate()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer from constrained, got {model.state}"
        )

    def test_force_activate_from_overridden(self):
        """Forced sensor bypasses overridden state."""
        model = _build_model()
        model.override()
        assert model.state == "overridden"

        model.force_activate()
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer from overridden, got {model.state}"
        )

    def test_forced_sensor_state_change_triggers_force_activate(self):
        """forced_sensor_state_change callback calls force_activate on sensor-on."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)
        model.sensor_on()
        assert model.state == "blocked"

        ev = MagicMock()
        ev.data = {
            "entity_id": "binary_sensor.forced",
            "old_state": MagicMock(state="off"),
            "new_state": MagicMock(state="on", context=MagicMock(id="ctx1")),
        }
        model.forced_sensor_state_change(ev)
        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer after forced sensor on, got {model.state}"
        )

    def test_forced_sensor_attribute_only_change_ignored(self):
        """Attribute-only changes on forced sensors must not trigger force_activate."""
        model = _build_model()
        ev = MagicMock()
        ev.data = {
            "entity_id": "binary_sensor.forced",
            "old_state": MagicMock(state="on"),
            "new_state": MagicMock(state="on", context=MagicMock(id="ctx2")),
        }
        model.forced_sensor_state_change(ev)
        assert model.state == "idle"

    def test_config_forced_sensor_entities_registers_listener(self):
        """config_forced_sensor_entities registers a state-change listener."""
        hass = _make_hass()
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as mock_track:
            from custom_components.entity_controller import Model
            m = Model.__new__(Model)
            m.log = logging.getLogger("test")
            m.forcedSensorEntities = []
            m.hass = hass
            m.config_forced_sensor_entities(
                {"forced_sensors": ["binary_sensor.fs1", "binary_sensor.fs2"]}
            )
            mock_track.assert_called_once_with(
                hass,
                ["binary_sensor.fs1", "binary_sensor.fs2"],
                m.forced_sensor_state_change,
            )

    def test_config_forced_sensor_entities_empty(self):
        """No listener when forced_sensors is empty."""
        hass = _make_hass()
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as mock_track:
            from custom_components.entity_controller import Model
            m = Model.__new__(Model)
            m.log = logging.getLogger("test")
            m.forcedSensorEntities = []
            m.hass = hass
            m.config_forced_sensor_entities({"forced_sensors": []})
            mock_track.assert_not_called()


# ---------------------------------------------------------------------------
# Phase 6 — HA bus event sensors
# ---------------------------------------------------------------------------

class TestEventBusSensors:

    def test_config_event_sensors_subscribes_to_bus(self):
        hass = _make_hass()
        hass.bus.async_listen = MagicMock(return_value=MagicMock())

        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m.eventSensorTypes = []
        m._event_sensor_cancel_callbacks = []
        m.hass = hass

        m.config_event_sensors({"event_sensors": ["evt_a", "evt_b"]})

        assert m.eventSensorTypes == ["evt_a", "evt_b"]
        assert hass.bus.async_listen.call_count == 2
        registered = [c[0][0] for c in hass.bus.async_listen.call_args_list]
        assert "evt_a" in registered
        assert "evt_b" in registered

    def test_config_event_sensors_empty(self):
        hass = _make_hass()
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m.eventSensorTypes = []
        m._event_sensor_cancel_callbacks = []
        m.hass = hass

        m.config_event_sensors({"event_sensors": []})

        hass.bus.async_listen.assert_not_called()
        assert m.eventSensorTypes == []

    def test_ha_event_sensor_callback_activates_from_idle(self):
        model = _build_model()
        assert model.state == "idle"
        model.is_state_entities_off = MagicMock(return_value=True)
        model.is_state_entities_on = MagicMock(return_value=False)

        model.ha_event_sensor_callback("my_event", MagicMock())

        assert model.state in ("active", "active_timer"), (
            f"Expected active/active_timer after bus event from idle, got {model.state}"
        )

    def test_ha_event_sensor_callback_ignored_from_overridden(self):
        """Regular bus event sensors respect the overridden state (unlike forced sensors)."""
        model = _build_model()
        model.override()
        assert model.state == "overridden"

        model.ha_event_sensor_callback("my_event", MagicMock())

        assert model.state == "overridden", (
            f"Expected overridden after bus event (not a forced sensor), got {model.state}"
        )

    def test_cancel_callbacks_called_on_reconfiguration(self):
        """Previous cancel callbacks must fire when config_event_sensors is called again."""
        hass = _make_hass()
        cancel1, cancel2 = MagicMock(), MagicMock()
        hass.bus.async_listen = MagicMock(return_value=MagicMock())

        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m.eventSensorTypes = []
        m._event_sensor_cancel_callbacks = [cancel1, cancel2]
        m.hass = hass

        m.config_event_sensors({"event_sensors": ["new_event"]})

        cancel1.assert_called_once()
        cancel2.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 2 — state persistence helpers
# ---------------------------------------------------------------------------

class TestStatePersistence:

    def test_storage_key_unique_per_instance(self):
        from custom_components.entity_controller import Model
        m1, m2 = Model.__new__(Model), Model.__new__(Model)
        m1.name, m2.name = "Living Room", "Kitchen"
        assert m1._storage_key() != m2._storage_key()

    def test_storage_key_starts_with_prefix(self):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.name = "Test EC"
        assert m._storage_key().startswith("entity_controller_state_")

    def test_storage_key_safe_characters_only(self):
        import re
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.name = "My EC #1 (Special)"
        suffix = m._storage_key()[len("entity_controller_state_"):]
        assert re.match(r'^[a-z0-9_]+$', suffix), f"Unsafe suffix: {suffix!r}"

    def test_schedule_save_noop_when_store_none(self):
        from custom_components.entity_controller import Model
        hass = _make_hass()
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m._store = None
        m.hass = hass
        m._schedule_save_state()
        hass.async_create_task.assert_not_called()

    def test_schedule_save_creates_task_when_store_set(self):
        from custom_components.entity_controller import Model
        hass = _make_hass()
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m._store = MagicMock()
        m.hass = hass
        m.state = "overridden"
        with patch(
            "custom_components.entity_controller.asyncio.run_coroutine_threadsafe"
        ) as schedule:
            m._schedule_save_state()
            schedule.assert_called_once()
            coro, loop = schedule.call_args.args
            assert asyncio.iscoroutine(coro)
            coro.close()
            assert loop is hass.loop

    def test_async_restore_false_when_store_none(self):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m._store = None
        result = asyncio.run(m._async_restore_state())
        assert result is False

    def test_async_restore_false_when_no_data(self):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        store = AsyncMock()
        store.async_load = AsyncMock(return_value=None)
        m._store = store
        result = asyncio.run(m._async_restore_state())
        assert result is False

    def test_async_restore_overridden_when_still_active(self):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m.overrideEntities = ["input_boolean.ov"]
        m.is_override_state_on = MagicMock(return_value=True)
        m.override = MagicMock()
        m.update = MagicMock()
        store = AsyncMock()
        store.async_load = AsyncMock(return_value={"state": "overridden", "saved_at": "x"})
        m._store = store
        result = asyncio.run(m._async_restore_state())
        assert result is True
        m.override.assert_called_once()

    def test_async_restore_overridden_falls_through_when_cleared(self):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m.overrideEntities = ["input_boolean.ov"]
        m.is_override_state_on = MagicMock(return_value=False)
        m.override = MagicMock()
        store = AsyncMock()
        store.async_load = AsyncMock(return_value={"state": "overridden", "saved_at": "x"})
        m._store = store
        result = asyncio.run(m._async_restore_state())
        assert result is False
        m.override.assert_not_called()

    def test_async_restore_idle_falls_through(self):
        from custom_components.entity_controller import Model
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        store = AsyncMock()
        store.async_load = AsyncMock(return_value={"state": "idle", "saved_at": "x"})
        m._store = store
        result = asyncio.run(m._async_restore_state())
        assert result is False

    NO_SAVE = "custom_components.entity_controller.asyncio.run_coroutine_threadsafe"
    LATER = "custom_components.entity_controller.event.async_call_later"

    def _restore_model(self, saved, live):
        """A model still in pending (startup) with `saved` in its store."""
        model = _build_model()
        model.to_pending()
        model.context = None  # production: nothing sets it before the restore
        model.stateEntities = ["light.a", "light.b"]
        model.controlEntities = ["light.a", "light.b"]
        model.block_timeout = 600
        model.turn_on_control_entities = MagicMock()
        model.turn_off_control_entities = MagicMock()
        model.hass.states.get = MagicMock(
            side_effect=lambda e: MagicMock(state=live[e]) if e in live else None)
        store = AsyncMock()
        store.async_load = AsyncMock(return_value=saved)
        model._store = store
        return model

    def _blocked_restore_model(self, lit, extra=None):
        live = {"light.a": "on" if lit else "off", "light.b": "off", **(extra or {})}
        return self._restore_model(
            {"state": "blocked", "saved_at": "x", "expires_at": None}, live)

    def _timer_restore_model(self, expires_in, live):
        from datetime import timedelta
        expiry = datetime.now() + timedelta(seconds=expires_in)
        model = self._restore_model(
            {"state": "active_timer", "saved_at": "x", "expires_at": str(expiry)}, live)
        return model, expiry

    def _restore(self, model):
        """Run the restore; returns (result, Timer mock, async_call_later mock)."""
        with patch("custom_components.entity_controller.Timer") as timer, patch(
            self.NO_SAVE, side_effect=lambda coro, loop: coro.close()
        ), patch(self.LATER) as later:
            result = asyncio.run(model._async_restore_state())
        return result, timer, later

    def test_async_restore_blocked_with_a_light_on_leaves_it_on(self):
        """A restored block keeps the lights as they are and re-arms the block
        timer. Before 9.11.3 the restore went through idle, whose on_enter_idle
        switched every light off, and still ended in blocked."""
        model = self._blocked_restore_model(lit=True)
        result, timer, _ = self._restore(model)
        assert result is True
        assert model.state == "blocked"
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        timer.assert_called_once_with(600, model.block_timer_expire)
        timer.return_value.start.assert_called_once()
        assert model.context.id.startswith("ec_")

    def test_async_restore_blocked_with_lights_off_goes_idle(self):
        model = self._blocked_restore_model(lit=False)
        result, timer, _ = self._restore(model)
        assert result is False
        assert model.state == "pending"
        model.start_monitoring()  # what startup_delay_callback does on False
        assert model.state == "idle"
        timer.assert_not_called()

    def test_async_restore_blocked_needs_block_enabled(self):
        model = self._blocked_restore_model(lit=True)
        model.disable_block = True
        result, timer, _ = self._restore(model)
        assert result is False
        assert model.state == "pending"
        timer.assert_not_called()

    def test_async_restore_blocked_defers_to_an_override_that_is_on(self):
        model = self._blocked_restore_model(lit=True, extra={"input_boolean.ov": "on"})
        model.overrideEntities = ["input_boolean.ov"]
        result, timer, _ = self._restore(model)
        assert result is False
        assert model.state == "pending"
        timer.assert_not_called()
        with patch(self.NO_SAVE, side_effect=lambda coro, loop: coro.close()):
            model.override()  # what startup_delay_callback does with an override on
        assert model.state == "overridden"
        model.turn_off_control_entities.assert_not_called()

    def test_restored_block_expiry_relights_with_ec_context(self):
        """EC's first turn_on after a restored block carries EC's context, so
        its echo is EC's own: no debounce window and no re-block."""
        model = self._blocked_restore_model(lit=True)
        del model.turn_on_control_entities
        result, _, _ = self._restore(model)
        assert result is True
        with patch(self.NO_SAVE, side_effect=lambda coro, loop: coro.close()), patch(
            self.LATER
        ) as later:
            model.block_timer_expires()
            assert model.state == "active_timer"
            calls = model.hass.services.async_call.call_args_list
            assert [c.args[1] for c in calls] == ["turn_on", "turn_on"]
            assert all(c.kwargs["context"].id.startswith("ec_") for c in calls)
            echo = MagicMock()
            echo.data = {
                "entity_id": "light.b",
                "old_state": MagicMock(state="off", attributes={}),
                "new_state": MagicMock(
                    state="on", attributes={}, context=calls[-1].kwargs["context"]),
            }
            model.state_entity_state_change(echo)
            later.assert_not_called()
        assert model.state == "active_timer"

    def test_async_restore_timer_with_time_left_resumes_it(self):
        """A running timer resumes with the time it had left and the lights
        untouched. Before 9.11.3 the restore returned False and the caller's
        start_monitoring() switched the lights off in on_enter_idle."""
        model, expiry = self._timer_restore_model(3600, {"light.a": "on", "light.b": "off"})
        del model._start_timer  # the real timer bookkeeping, Timer patched
        result, timer, later = self._restore(model)
        assert result is True
        assert model.state == "active_timer"
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        timer.assert_called_once()
        delay, expire_cb = timer.call_args[0]
        assert 3000 < delay <= 3600  # the time left, not the configured 180 s
        assert expire_cb == model.timer_expire
        timer.return_value.start.assert_called_once()
        assert abs((model._expiry_time - expiry).total_seconds()) < 600
        assert getattr(model, "_pending_restore_expiry", None) is None
        later.assert_not_called()
        assert model.context.id.startswith("ec_")

    def test_restored_timer_runs_out_with_ec_context(self):
        model, _ = self._timer_restore_model(3600, {"light.a": "on", "light.b": "on"})
        del model.turn_off_control_entities
        result, _, _ = self._restore(model)
        assert result is True
        with patch(self.NO_SAVE, side_effect=lambda coro, loop: coro.close()):
            model.timer_expire()  # what the resumed Timer calls when it runs out
        assert model.state == "idle"
        calls = model.hass.services.async_call.call_args_list
        assert [c.args[1] for c in calls] == ["turn_off", "turn_off"]
        assert all(c.kwargs["context"].id.startswith("ec_") for c in calls)

    def test_async_restore_timer_waits_for_an_unavailable_entity(self):
        """Unchanged (9.9.1): an entity that is not up yet defers the decision."""
        model, expiry = self._timer_restore_model(
            3600, {"light.a": "unavailable", "light.b": "on"})
        result, timer, later = self._restore(model)
        assert result is False
        assert model.state == "pending"
        assert model._pending_restore_expiry == expiry
        later.assert_called_once_with(model.hass, 30, model._restore_timer_finish)
        timer.assert_not_called()

    def test_async_restore_expired_timer_turns_off(self):
        """Unchanged: a timer that ran out while HA was down is finished."""
        model, _ = self._timer_restore_model(-60, {"light.a": "on", "light.b": "on"})
        result, timer, _ = self._restore(model)
        assert result is True
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called()
        timer.assert_not_called()

    def test_async_restore_timer_with_lights_off_goes_idle(self):
        """Unchanged: nothing left to finish."""
        model, _ = self._timer_restore_model(3600, {"light.a": "off", "light.b": "off"})
        result, timer, later = self._restore(model)
        assert result is False
        assert model.state == "pending"
        later.assert_not_called()
        timer.assert_not_called()

    @pytest.mark.parametrize("expires_in", [3600, -60], ids=["time-left", "expired"])
    def test_async_restore_timer_defers_to_an_override_that_is_on(self, expires_in):
        model, _ = self._timer_restore_model(
            expires_in, {"light.a": "on", "light.b": "on", "input_boolean.ov": "on"})
        model.overrideEntities = ["input_boolean.ov"]
        result, timer, _ = self._restore(model)
        assert result is False
        assert model.state == "pending"
        timer.assert_not_called()
        with patch(self.NO_SAVE, side_effect=lambda coro, loop: coro.close()):
            model.override()  # what startup_delay_callback does with an override on
        assert model.state == "overridden"
        model.turn_off_control_entities.assert_not_called()

    def test_on_enter_overridden_schedules_save(self):
        model = _build_model()
        model._store = MagicMock()
        model.override()
        model.hass.async_create_task.assert_called()

    def test_on_enter_blocked_schedules_save(self):
        model = _build_model()
        model._store = MagicMock()
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)
        with patch(
            "custom_components.entity_controller.asyncio.run_coroutine_threadsafe"
        ) as schedule:
            model.sensor_on()
            assert model.state == "blocked"
            schedule.assert_called()

    def test_schedule_save_is_thread_safe(self):
        """Regression test: _schedule_save_state must work from a worker thread.

        on_enter_blocked starts a threading.Timer whose block_timer_expire
        callback drives on_exit_blocked from a worker thread. Calling
        hass.async_create_task off-loop on HA 2026.x raises RuntimeError,
        which aborts the transition and traps the controller in ``blocked``.
        """
        import threading
        from custom_components.entity_controller import Model
        hass = _make_hass()
        m = Model.__new__(Model)
        m.log = logging.getLogger("test")
        m._store = MagicMock()
        m.hass = hass
        m.state = "blocked"
        errors = []

        def call_from_worker():
            try:
                m._schedule_save_state()
            except Exception as e:  # pragma: no cover - failure path
                errors.append(e)

        t = threading.Thread(target=call_from_worker)
        t.start()
        t.join(timeout=2)
        assert errors == [], f"_schedule_save_state raised from worker: {errors}"
        hass.async_create_task.assert_not_called()


# ---------------------------------------------------------------------------
# grace_period — suppress state-entity feedback from slow cloud integrations
# ---------------------------------------------------------------------------

class TestGracePeriod:
    """Tests for the grace_period configuration option.

    grace_period is a fallback for integrations (e.g. Tahoma/Somfy) that do not
    propagate the original HA context to their state-change events, causing EC to
    enter the blocked state on its own delayed state feedback.
    """

    # ------------------------------------------------------------------
    # is_within_grace_period() unit tests
    # ------------------------------------------------------------------

    def test_is_within_grace_period_disabled_when_none(self):
        """is_within_grace_period() must return False when grace_period is None."""
        model = _build_model()
        assert model.grace_period is None
        assert model.is_within_grace_period() is False

    def test_is_within_grace_period_true_during_window(self):
        """is_within_grace_period() returns True while the window is active."""
        from datetime import timedelta
        model = _build_model()
        model.grace_period = 30
        # Set ignore_state_changes_until well into the future
        model.ignore_state_changes_until = datetime.now() + timedelta(seconds=30)
        assert model.is_within_grace_period() is True

    def test_is_within_grace_period_false_after_window(self):
        """is_within_grace_period() returns False once the deadline has passed."""
        from datetime import timedelta
        model = _build_model()
        model.grace_period = 30
        # Set ignore_state_changes_until to a time in the past
        model.ignore_state_changes_until = datetime.now() - timedelta(seconds=1)
        assert model.is_within_grace_period() is False

    # ------------------------------------------------------------------
    # call_service() – sets ignore_state_changes_until
    # ------------------------------------------------------------------

    def test_call_service_sets_deadline_when_grace_period_configured(self):
        """call_service() updates ignore_state_changes_until when grace_period is set."""
        from datetime import timedelta
        model = _build_model()
        model.grace_period = 5
        before = datetime.now()
        model.call_service("light.test", "turn_on")
        after = datetime.now()
        # ignore_state_changes_until must be between before+5s and after+5s
        assert model.ignore_state_changes_until >= before + timedelta(seconds=5)
        assert model.ignore_state_changes_until <= after + timedelta(seconds=5)

    def test_call_service_does_not_set_deadline_when_grace_period_none(self):
        """call_service() must NOT update ignore_state_changes_until when grace_period is None."""
        model = _build_model()
        model.grace_period = None
        original = model.ignore_state_changes_until
        model.call_service("light.test", "turn_on")
        # The timestamp must be unchanged
        assert model.ignore_state_changes_until == original

    # ------------------------------------------------------------------
    # state_entity_state_change() – events dropped inside the window
    # ------------------------------------------------------------------

    def _make_state_change_event(self, entity_id="light.test", old_state="off", new_state="on"):
        """Build a minimal HA state-change event mock."""
        ev = MagicMock()
        ev.data = {
            "entity_id": entity_id,
            "old_state": MagicMock(state=old_state, attributes={}),
            "new_state": MagicMock(
                state=new_state,
                attributes={},
                context=MagicMock(id="unrelated-ctx"),
            ),
        }
        return ev

    def test_state_entity_state_change_ignored_within_grace_period(self):
        """state_entity_state_change must be a no-op while within the grace period.

        Put the model in active_timer so state_entity_state_change would normally
        call control() and transition to blocked.  With grace_period active the
        event must be silently dropped and the state must remain active_timer.
        """
        from datetime import timedelta
        model = _build_model()
        # Reach active_timer via sensor_on (entities off, no block)
        model.is_state_entities_off = MagicMock(return_value=True)
        model.is_state_entities_on = MagicMock(return_value=False)
        model.will_stay_on = MagicMock(return_value=False)
        model.sensor_on()
        assert model.state == "active_timer"

        model.grace_period = 30
        model.ignore_state_changes_until = datetime.now() + timedelta(seconds=30)
        model.is_ignored_context = MagicMock(return_value=False)
        # State entity turns ON (own delayed feedback that would normally trigger blocked)
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        ev = self._make_state_change_event(old_state="off", new_state="on")
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.state_entity_state_change(ev)
        later.assert_not_called()

        # Grace period must have suppressed the transition
        assert model.state == "active_timer", (
            f"Expected active_timer (grace period suppressed), got {model.state}"
        )

    def test_state_entity_state_change_processed_after_grace_period(self):
        """state_entity_state_change must proceed normally after the grace period expires.

        Same scenario as above but with an expired deadline – EC must transition
        to blocked because an external device turned on the state entity.
        """
        from datetime import timedelta
        model = _build_model()
        # Reach active_timer
        model.is_state_entities_off = MagicMock(return_value=True)
        model.is_state_entities_on = MagicMock(return_value=False)
        model.will_stay_on = MagicMock(return_value=False)
        model.sensor_on()
        assert model.state == "active_timer"

        model.grace_period = 30
        # Grace period expired 1 second ago
        model.ignore_state_changes_until = datetime.now() - timedelta(seconds=1)
        model.is_ignored_context = MagicMock(return_value=False)
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)

        ev = self._make_state_change_event(old_state="off", new_state="on")
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.state_entity_state_change(ev)
            assert model.state == "active_timer"
            later.assert_called_once()
            model.hass.states.get = MagicMock(return_value=MagicMock(state="on", attributes={}))
            later.call_args[0][2](None)

        # EC must have reacted – blocked because state entity is on and block enabled
        assert model.state == "blocked", (
            f"Expected blocked after grace period expired, got {model.state}"
        )

    # ------------------------------------------------------------------
    # Integration scenario: cloud gateway produces delayed feedback
    # ------------------------------------------------------------------

    def test_grace_period_prevents_blocked_on_own_delayed_feedback(self):
        """Cloud integrations (e.g. Tahoma) deliver state feedback with a fresh,
        unrelated context.  With grace_period set, EC must not enter blocked when
        it sees its own delayed on-state feedback after calling turn_on.

        Sequence:
          1. sensor fires → EC activates (active_timer)
          2. EC calls turn_on → grace_period window armed
          3. Cloud integration delivers delayed state feedback (fresh context)
          4. EC must stay in active_timer, NOT transition to blocked
        """
        from datetime import timedelta
        model = _build_model()
        # Step 1: Reach active_timer
        model.is_state_entities_off = MagicMock(return_value=True)
        model.is_state_entities_on = MagicMock(return_value=False)
        model.will_stay_on = MagicMock(return_value=False)
        model.sensor_on()
        assert model.state == "active_timer"

        # Step 2: Arm the grace-period window (simulates what call_service does)
        model.grace_period = 5
        model.ignore_state_changes_until = datetime.now() + timedelta(seconds=5)

        # Step 3: Delayed state feedback arrives with fresh, unrelated context
        model.is_ignored_context = MagicMock(return_value=False)
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)
        ev = self._make_state_change_event(old_state="off", new_state="on")
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.state_entity_state_change(ev)
        later.assert_not_called()

        # Step 4: Grace period must have suppressed the blocked transition
        assert model.state == "active_timer", (
            f"Expected active_timer (grace period suppressed), got {model.state}"
        )


# ---------------------------------------------------------------------------
# Phase 4 – start_time transition must not raise MachineError
# ---------------------------------------------------------------------------

class TestStartTimeTransition:
    """Regression tests for the start_time_callback error storm (2026-07-08).

    ``start_time_callback`` calls ``blocked()``/``enable()`` regardless of the
    current machine state. When the machine was ``idle`` at start_time (HA
    restarted inside the active window, or an override toggled off while
    constrained) ``blocked()`` raised ``MachineError`` — and HA re-fires a
    failed point-in-time callback endlessly, flooding the log.
    """

    def _prepare(self, state, entities_on=True, block_enabled=True):
        model = _build_model()
        model.state = state
        model.is_state_entities_on = MagicMock(return_value=entities_on)
        model.is_state_entities_off = MagicMock(return_value=not entities_on)
        model.is_block_enabled = MagicMock(return_value=block_enabled)
        model.is_override_state_on = MagicMock(return_value=False)
        model.is_override_state_off = MagicMock(return_value=True)
        return model

    def test_blocked_from_idle(self):
        """idle + entities on + block enabled → blocked, without MachineError."""
        model = self._prepare("idle")
        model._apply_start_time_transition()
        assert model.state == "blocked"

    def test_blocked_from_constrained_still_works(self):
        """Existing constrained → blocked path is unchanged."""
        model = self._prepare("constrained")
        model._apply_start_time_transition()
        assert model.state == "blocked"

    def test_enable_from_constrained_when_entities_off(self):
        """entities off → enable() → idle (normal start-of-window path)."""
        model = self._prepare("constrained", entities_on=False)
        model._apply_start_time_transition()
        assert model.state == "idle"

    def test_overridden_does_not_raise_and_keeps_state(self):
        """No transition is defined from overridden: must log, not raise."""
        model = self._prepare("overridden")
        model._apply_start_time_transition()
        assert model.state == "overridden"


# ---------------------------------------------------------------------------
# Phase 5 – futurize() must compare in HA-local wall time, not the OS timezone
# ---------------------------------------------------------------------------

class TestFuturizeTimezone:
    """Regression tests for the futurize() timezone bug (2026-07-09).

    parse_time() returns naive times in the HA-configured timezone, but
    futurize() compared them against datetime.now()/date.today() in the OS
    timezone. On a host running UTC with HA on Europe/Prague, a sunrise
    end_time callback rescheduled itself to a wall time that was future in
    UTC terms but already past locally — async_track_point_in_time fired it
    again immediately, in an endless turn_off loop for exactly the UTC-offset
    window.
    """

    def _futurize_at(self, local_now, timet):
        """Run futurize() with dt.now() mocked to a fixed HA-local instant."""
        from zoneinfo import ZoneInfo
        import homeassistant.util.dt as dt_util
        prague = ZoneInfo("Europe/Prague")
        model = _build_model()
        previous_tz = dt_util.DEFAULT_TIME_ZONE
        dt_util.set_default_time_zone(prague)
        try:
            fixed = local_now.replace(tzinfo=prague)
            with patch(
                "custom_components.entity_controller.dt.now",
                return_value=fixed,
            ):
                return model.futurize(timet)
        finally:
            dt_util.set_default_time_zone(previous_tz)

    def test_past_local_time_returns_tomorrow(self):
        """05:23 local with local now 06:47 → tomorrow 05:23 (the incident case:
        OS-UTC now was 04:47, so the old code kept it 'today' and re-fired)."""
        from datetime import datetime as dtm, time as tm
        result = self._futurize_at(dtm(2026, 7, 9, 6, 47, 0), tm(5, 23, 0))
        assert result == dtm(2026, 7, 10, 5, 23, 0)

    def test_future_local_time_stays_today(self):
        from datetime import datetime as dtm, time as tm
        result = self._futurize_at(dtm(2026, 7, 9, 6, 47, 0), tm(23, 50, 0))
        assert result == dtm(2026, 7, 9, 23, 50, 0)

    def test_past_naive_datetime_input_returns_tomorrow(self):
        from datetime import datetime as dtm
        result = self._futurize_at(
            dtm(2026, 7, 9, 6, 47, 0), dtm(2026, 7, 9, 5, 23, 0)
        )
        assert result == dtm(2026, 7, 10, 5, 23, 0)

    def test_aware_datetime_input_is_normalized(self):
        from datetime import datetime as dtm
        from zoneinfo import ZoneInfo
        aware = dtm(2026, 7, 9, 5, 23, 0, tzinfo=ZoneInfo("Europe/Prague"))
        result = self._futurize_at(dtm(2026, 7, 9, 6, 47, 0), aware)
        assert result == dtm(2026, 7, 10, 5, 23, 0)


# ---------------------------------------------------------------------------
# Override at start_time — an override armed while constrained must survive
# ---------------------------------------------------------------------------

class TestStartTimeOverride:
    """Regression tests for the override lost at start_time (2026-07-29).

    ``override_state_change()`` only calls ``override()`` from
    active/active_timer/idle/blocked — mirroring the ``override`` trigger,
    which has no ``constrained`` source — so an override switching on while
    the controller is still ``constrained`` is dropped. When the controlled
    entity then happened to be on at ``start_time``, ``_apply_start_time_transition``
    took the ``blocked()`` branch, which never consults the override; once the
    block cleared the machine landed in ``idle`` = armed, and the room lit up
    on presence all night (observed on ec_202, 7 times between 21:43 and 23:31).
    """

    def _prepare(self, state, entities_on=True, override_on=True,
                 overrides=("input_boolean.disable_ec_test",), block_enabled=True):
        model = _build_model()
        model.state = state
        model.overrideEntities = list(overrides)
        model.is_state_entities_on = MagicMock(return_value=entities_on)
        model.is_state_entities_off = MagicMock(return_value=not entities_on)
        model.is_block_enabled = MagicMock(return_value=block_enabled)
        model.is_override_state_on = MagicMock(return_value=override_on)
        model.is_override_state_off = MagicMock(return_value=not override_on)
        model._override_entity_state = MagicMock(
            return_value=list(overrides)[0] if (override_on and overrides) else None
        )
        return model

    def test_override_on_with_entities_on_goes_overridden(self):
        """THE FIX: constrained + override on + entities on → overridden, not blocked."""
        model = self._prepare("constrained", entities_on=True)
        model._apply_start_time_transition()
        assert model.state == "overridden", (
            f"override was armed while constrained but landed in {model.state}"
        )

    def test_override_on_with_entities_off_still_overridden(self):
        """Path that already worked before the fix must be unaffected."""
        model = self._prepare("constrained", entities_on=False)
        model._apply_start_time_transition()
        assert model.state == "overridden"

    def test_no_override_entities_configured_still_blocks(self):
        """A controller without overrides must keep the original behaviour."""
        model = self._prepare("constrained", entities_on=True, override_on=False,
                              overrides=())
        model._apply_start_time_transition()
        assert model.state == "blocked"

    def test_override_off_still_blocks(self):
        """Overrides configured but all off → original blocked path."""
        model = self._prepare("constrained", entities_on=True, override_on=False)
        model._apply_start_time_transition()
        assert model.state == "blocked"

    def test_override_on_from_idle_keeps_original_path(self):
        """The new branch is scoped to `constrained` on purpose.

        start_time_callback also runs while already idle (see Phase 4); those
        states are reached only after override_state_change() has had its
        chance, so they must keep the idle → blocked behaviour.
        """
        model = self._prepare("idle", entities_on=True)
        model._apply_start_time_transition()
        assert model.state == "blocked"

    def test_overridden_by_and_at_are_recorded(self):
        """Attributes must match what override_state_change() would have set."""
        model = self._prepare("constrained", entities_on=True)
        model.update = MagicMock(wraps=model.update)
        model._apply_start_time_transition()
        keys = {}
        for call in model.update.call_args_list:
            keys.update(call.kwargs)
        assert keys.get("overridden_by") == "input_boolean.disable_ec_test"
        assert "overridden_at" in keys


# ---------------------------------------------------------------------------
# Lux constraint — illuminance-gated activation
# ---------------------------------------------------------------------------

class TestLuxConstraint:

    def _prepare(self, lux_state=None, threshold=100.0):
        """Model in idle with entities off; lux entity configured when
        lux_state is not None."""
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=False)
        model.is_state_entities_off = MagicMock(return_value=True)
        if lux_state is not None:
            model.luxEntity = "sensor.test_illuminance"
            model.lux_threshold = threshold
            state_obj = MagicMock()
            state_obj.state = lux_state
            model.hass.states.get = MagicMock(return_value=state_obj)
        return model

    def test_activation_allowed_when_dark(self):
        """Below the threshold the original idle → active path must fire."""
        model = self._prepare(lux_state="30")
        model.sensor_on()
        assert model.state in ("active", "active_timer"), (
            f"Expected activation at 30 lx < 100 lx, got {model.state}"
        )

    def test_activation_blocked_when_bright(self):
        """At or above the threshold motion must not turn the lights on."""
        model = self._prepare(lux_state="450")
        model.sensor_on()
        assert model.state == "idle", (
            f"Expected idle at 450 lx >= 100 lx, got {model.state}"
        )

    def test_threshold_is_inclusive_upper_bound(self):
        """Exactly at the threshold counts as bright (activation blocked)."""
        model = self._prepare(lux_state="100")
        model.sensor_on()
        assert model.state == "idle"

    def test_no_lux_config_allows_activation(self):
        """Controllers without a lux constraint keep pre-lux behaviour."""
        model = self._prepare()  # luxEntity/lux_threshold stay None
        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_unavailable_sensor_fails_open(self):
        """A non-numeric state (unavailable/unknown) must not block lights."""
        model = self._prepare(lux_state="unavailable")
        model.sensor_on()
        assert model.state in ("active", "active_timer"), (
            f"Expected fail-open activation on unavailable sensor, got {model.state}"
        )

    def test_missing_entity_fails_open(self):
        """A lux entity that does not exist in HA must not block lights."""
        model = self._prepare(lux_state="whatever")
        model.hass.states.get = MagicMock(return_value=None)
        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_bright_room_does_not_gate_timer_reset(self):
        """Once active, sensor_on must keep resetting the timer even when the
        (now lit) room reads above the threshold."""
        model = self._prepare(lux_state="30")
        model.sensor_on()
        assert model.state == "active_timer"

        # The controlled light pushed the reading past the threshold
        state_obj = MagicMock()
        state_obj.state = "800"
        model.hass.states.get = MagicMock(return_value=state_obj)
        model._reset_timer = MagicMock(return_value=True)

        model.sensor_on()
        assert model.state == "active_timer", (
            f"Expected re-trigger to stay in active_timer, got {model.state}"
        )
        model._reset_timer.assert_called_once()

    def test_bright_room_does_not_gate_blocked_path(self):
        """idle → blocked (entities already on) must ignore the lux reading."""
        model = self._prepare(lux_state="800")
        model.is_state_entities_on = MagicMock(return_value=True)
        model.is_state_entities_off = MagicMock(return_value=False)
        model.is_block_enabled = MagicMock(return_value=True)
        model.sensor_on()
        assert model.state == "blocked"

    def test_activate_service_bypasses_lux(self):
        """The explicit activate service is a manual command — never gated."""
        model = self._prepare(lux_state="800")
        model.activate()
        assert model.state in ("active", "active_timer")

    def test_force_activate_bypasses_lux(self):
        """Forced sensors bypass every constraint, lux included."""
        model = self._prepare(lux_state="800")
        model.force_activate()
        assert model.state in ("active", "active_timer")

    def test_blocked_activation_records_attributes(self):
        """A lux-blocked activation must leave a diagnostic trail."""
        model = self._prepare(lux_state="450")
        model.update = MagicMock(wraps=model.update)
        model.sensor_on()
        keys = {}
        for call in model.update.call_args_list:
            keys.update(call.kwargs)
        assert keys.get("lux_at_last_block") == 450.0
        assert "lux_blocked_at" in keys

    def test_config_lux_constraint_both_set(self):
        model = _build_model()
        model.config_lux_constraint(
            {"lux_entity": "sensor.room_illuminance", "lux_threshold": 75}
        )
        assert model.luxEntity == "sensor.room_illuminance"
        assert model.lux_threshold == 75.0

    def test_config_lux_constraint_entity_without_threshold_disabled(self):
        model = _build_model()
        model.config_lux_constraint({"lux_entity": "sensor.room_illuminance"})
        assert model.luxEntity is None
        assert model.is_lux_constraint_satisfied() is True

    def test_config_lux_constraint_threshold_without_entity_disabled(self):
        model = _build_model()
        model.config_lux_constraint({"lux_threshold": 75})
        assert model.lux_threshold is None
        assert model.is_lux_constraint_satisfied() is True

    def test_lux_condition_present_in_machine(self):
        """The idle → active sensor_on transition must carry the lux condition
        alongside is_state_entities_off (and only that transition)."""
        from transitions.extensions import HierarchicalMachine as Machine
        from custom_components.entity_controller.const import STATES

        machine = Machine(states=STATES, initial="pending", finalize_event="finalize")
        _add_machine_transitions(machine)

        def _condition_names(t):
            names = []
            for c in t.conditions:
                f = c.func
                names.append(f.__name__ if callable(f) else str(f))
            return names

        idle_active = [
            t for t in machine.get_transitions("sensor_on")
            if t.source == "idle" and t.dest == "active"
        ]
        gated = [
            t for t in idle_active
            if "is_lux_constraint_satisfied" in _condition_names(t)
        ]
        assert len(gated) == 1, (
            "Expected exactly one lux-gated sensor_on idle → active transition"
        )
        assert "is_state_entities_off" in _condition_names(gated[0]), (
            "The lux condition must gate the entities-off activation path"
        )


    # -- stale-reading re-check (2026-09-04) --------------------------------

    def _sensor_on_event(self):
        ev = MagicMock()
        ev.data = {
            "entity_id": "binary_sensor.motion",
            "old_state": MagicMock(state="off"),
            "new_state": MagicMock(state="on", context=MagicMock(id="ctx-lux")),
        }
        return ev

    def _states(self, model, lux, pir):
        """hass.states.get returning the PIR for the sensor, lux otherwise."""
        pir_obj = MagicMock(state=pir)
        lux_obj = MagicMock(state=lux)
        model.hass.states.get = MagicMock(
            side_effect=lambda e: pir_obj if e == "binary_sensor.motion" else lux_obj
        )

    def test_blocked_activation_schedules_one_recheck(self):
        """A lux-blocked sensor_on from idle schedules exactly one re-check,
        even when the sensor fires again while it is pending."""
        model = self._prepare(lux_state="450")
        self._states(model, lux="450", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.sensor_state_change(self._sensor_on_event())
            assert model.state == "idle"
            later.assert_called_once()
            assert later.call_args[0][1] == 1.0
            model.sensor_state_change(self._sensor_on_event())
            later.assert_called_once()

    def test_dark_activation_schedules_no_recheck(self):
        """A normal activation must not leave a re-check behind."""
        model = self._prepare(lux_state="30")
        self._states(model, lux="30", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.sensor_state_change(self._sensor_on_event())
            assert model.state in ("active", "active_timer")
            later.assert_not_called()

    def test_recheck_activates_when_fresh_reading_is_dark(self):
        """The late illuminance report says dark and the PIR is still on:
        the re-check completes the activation the first pass refused."""
        model = self._prepare(lux_state="450")
        self._states(model, lux="450", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later"):
            model.sensor_state_change(self._sensor_on_event())
        assert model.state == "idle"
        self._states(model, lux="6", pir="on")
        model._lux_recheck(None)
        assert model.state in ("active", "active_timer"), (
            f"Expected activation on re-check at 6 lx, got {model.state}"
        )
        assert model._lux_recheck_handle is None

    def test_recheck_keeps_bright_room_dark(self):
        """A room that really is bright stays blocked after the re-check."""
        model = self._prepare(lux_state="450")
        self._states(model, lux="450", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later"):
            model.sensor_state_change(self._sensor_on_event())
        model._lux_recheck(None)
        assert model.state == "idle"

    def test_recheck_noop_when_sensor_already_off(self):
        """No sensor on any more -> nothing to retry, even if it is dark now."""
        model = self._prepare(lux_state="450")
        self._states(model, lux="450", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later"):
            model.sensor_state_change(self._sensor_on_event())
        self._states(model, lux="6", pir="off")
        model._lux_recheck(None)
        assert model.state == "idle"

    def test_recheck_noop_when_no_longer_idle(self):
        """A re-check that fires after the controller moved on is ignored."""
        model = self._prepare(lux_state="450")
        self._states(model, lux="450", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later"):
            model.sensor_state_change(self._sensor_on_event())
        model.to_blocked()
        self._states(model, lux="6", pir="on")
        model._lux_recheck(None)
        assert model.state == "blocked"

    def test_recheck_disabled_by_config(self):
        """lux_recheck_delay None keeps the pre-9.11.1 single-shot gate."""
        model = self._prepare(lux_state="450")
        self._states(model, lux="450", pir="on")
        model.lux_recheck_delay = None
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.sensor_state_change(self._sensor_on_event())
            assert model.state == "idle"
            later.assert_not_called()

    def test_bright_states_block_also_schedules_recheck(self):
        """The string-state flavour of the gate gets the same second look."""
        model = self._prepare()
        model.luxEntity = "sensor.radar_illumination"
        model.lux_bright_states = ["bright"]
        self._states(model, lux="bright", pir="on")
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.sensor_state_change(self._sensor_on_event())
            assert model.state == "idle"
            later.assert_called_once()


class TestLuxBrightStates:
    """String-state (radar bright/dim) flavour of the lux constraint."""

    def _prepare(self, lux_state, bright_states=("bright",), threshold=None):
        model = _build_model()
        model.is_state_entities_on = MagicMock(return_value=False)
        model.is_state_entities_off = MagicMock(return_value=True)
        model.luxEntity = "sensor.test_radar_illumination"
        model.lux_bright_states = list(bright_states)
        model.lux_threshold = threshold
        state_obj = MagicMock()
        state_obj.state = lux_state
        model.hass.states.get = MagicMock(return_value=state_obj)
        return model

    def test_bright_state_blocks_activation(self):
        model = self._prepare("bright")
        model.sensor_on()
        assert model.state == "idle", (
            f"Expected idle on 'bright' radar state, got {model.state}"
        )

    def test_dim_state_allows_activation(self):
        model = self._prepare("dim")
        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_unavailable_fails_open(self):
        model = self._prepare("unavailable")
        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_numeric_state_without_threshold_fails_open(self):
        """A numeric reading on a bright-states-only sensor must not block."""
        model = self._prepare("450")
        model.sensor_on()
        assert model.state in ("active", "active_timer")

    def test_combined_numeric_uses_threshold(self):
        """With both configured, numeric readings compare to the threshold."""
        model = self._prepare("450", threshold=100.0)
        model.sensor_on()
        assert model.state == "idle"

    def test_combined_string_uses_bright_states(self):
        """With both configured, string readings match lux_bright_states."""
        model = self._prepare("bright", threshold=100.0)
        model.sensor_on()
        assert model.state == "idle"

    def test_blocked_records_string_state(self):
        model = self._prepare("bright")
        model.update = MagicMock(wraps=model.update)
        model.sensor_on()
        keys = {}
        for call in model.update.call_args_list:
            keys.update(call.kwargs)
        assert keys.get("lux_at_last_block") == "bright"
        assert "lux_blocked_at" in keys

    def test_config_bright_states_without_threshold_is_valid(self):
        model = _build_model()
        model.config_lux_constraint(
            {"lux_entity": "sensor.radar_illumination",
             "lux_bright_states": ["bright"]}
        )
        assert model.luxEntity == "sensor.radar_illumination"
        assert model.lux_bright_states == ["bright"]
        assert model.lux_threshold is None

    def test_config_entity_without_any_criterion_disabled(self):
        model = _build_model()
        model.config_lux_constraint({"lux_entity": "sensor.radar_illumination"})
        assert model.luxEntity is None
        assert model.is_lux_constraint_satisfied() is True

    def test_config_bright_states_without_entity_disabled(self):
        model = _build_model()
        model.config_lux_constraint({"lux_bright_states": ["bright"]})
        assert model.lux_bright_states == []
        assert model.is_lux_constraint_satisfied() is True


# ---------------------------------------------------------------------------
# Night mode: entity-state-driven detection + alternate control entities
# ---------------------------------------------------------------------------

class TestNightModeEntity:
    """night_mode.entity/entity_states/entities (state-driven night mode)."""

    def _prepare(self, mode_state="noc", night_states=("vecerka", "noc"),
                 night_entities=("light.bed",), window=None):
        model = _build_model()
        model.controlEntities = ["light.downlights"]
        model.night_mode = {"delay": 120}
        if window:
            model.night_mode["start_time"] = window[0]
            model.night_mode["end_time"] = window[1]
        model.night_mode_entity = "input_select.house_mode"
        model.night_mode_entity_states = list(night_states)
        model.nightControlEntities = list(night_entities)
        model.light_params_night = {
            "delay": 120, "service_data": None, "service_data_off": None,
        }
        state_obj = MagicMock()
        state_obj.state = mode_state
        model.hass.states.get = MagicMock(return_value=state_obj)
        return model

    # --- is_night ---------------------------------------------------------

    def test_no_night_mode_is_never_night(self):
        model = _build_model()
        assert model.is_night() is False

    def test_entity_state_in_states_is_night(self):
        model = self._prepare("noc")
        assert model.is_night() is True

    def test_entity_state_vecerka_is_night(self):
        model = self._prepare("vecerka")
        assert model.is_night() is True

    def test_entity_state_not_in_states_is_day(self):
        model = self._prepare("vecer")
        assert model.is_night() is False

    def test_missing_entity_fails_open_to_day(self):
        model = self._prepare("noc")
        model.hass.states.get = MagicMock(return_value=None)
        assert model.is_night() is False

    def test_entity_day_but_time_window_says_night(self):
        """Entity and window coexist: whichever says night wins."""
        model = self._prepare("vecer", window=("22:00:00", "06:00:00"))
        model.now_is_between = MagicMock(return_value=True)
        assert model.is_night() is True

    def test_entity_night_short_circuits_window(self):
        model = self._prepare("noc", window=("22:00:00", "06:00:00"))
        model.now_is_between = MagicMock(return_value=False)
        assert model.is_night() is True

    # --- prepare_service_data / entity switching ---------------------------

    def test_night_activation_targets_night_entities(self):
        model = self._prepare("noc")
        model.prepare_service_data()
        assert model.activeControlEntities == ["light.bed"]
        assert model.lightParams is model.light_params_night

    def test_day_activation_targets_control_entities(self):
        model = self._prepare("vecer")
        model.prepare_service_data()
        assert model.activeControlEntities == ["light.downlights"]
        assert model.lightParams is model.light_params_day

    def test_night_without_night_entities_falls_back(self):
        model = self._prepare("noc", night_entities=())
        model.prepare_service_data()
        assert model.activeControlEntities == ["light.downlights"]

    def test_turn_on_calls_night_entities_only(self):
        model = self._prepare("noc")
        model.prepare_service_data()
        model.call_service = MagicMock()
        model.turn_on_control_entities()
        called = [c.args[0] for c in model.call_service.call_args_list]
        assert called == ["light.bed"]

    def test_turn_off_uses_set_chosen_at_activation(self):
        """Mode flip between activation and timeout must not orphan lights."""
        model = self._prepare("noc")
        model.prepare_service_data()  # activates with night set
        state_obj = MagicMock()
        state_obj.state = "den"  # house mode flips before the timer expires
        model.hass.states.get = MagicMock(return_value=state_obj)
        model.call_service = MagicMock()
        model.turn_off_control_entities()
        called = [c.args[0] for c in model.call_service.call_args_list]
        assert called == ["light.bed"]

    def test_empty_active_set_falls_back_to_control_entities(self):
        model = self._prepare("noc")
        model.activeControlEntities = []
        model.call_service = MagicMock()
        model.turn_on_control_entities()
        called = [c.args[0] for c in model.call_service.call_args_list]
        assert called == ["light.downlights"]

    # --- config_night_mode --------------------------------------------------

    def _config(self, model, night_mode):
        with patch(
            "custom_components.entity_controller.event.async_track_state_change_event"
        ) as track:
            model.config_night_mode({"night_mode": night_mode})
        return track

    def test_config_entity_mode_valid_without_window(self):
        model = _build_model()
        model.light_params_day = {"delay": 180, "service_data": None,
                                  "service_data_off": None}
        self._config(model, {
            "entity": "input_select.house_mode",
            "entity_states": ["vecerka", "noc"],
            "entities": ["light.bed"],
        })
        assert model.night_mode is not None
        assert model.night_mode_entity == "input_select.house_mode"
        assert model.nightControlEntities == ["light.bed"]

    def test_config_entity_without_states_disables(self):
        model = _build_model()
        model.light_params_day = {"delay": 180, "service_data": None,
                                  "service_data_off": None}
        self._config(model, {"entity": "input_select.house_mode"})
        assert model.night_mode is None
        assert model.is_night() is False

    def test_config_neither_window_nor_entity_disables(self):
        model = _build_model()
        model.light_params_day = {"delay": 180, "service_data": None,
                                  "service_data_off": None}
        self._config(model, {"delay": 60})
        assert model.night_mode is None

    def test_config_time_window_still_works(self):
        model = _build_model()
        model.light_params_day = {"delay": 180, "service_data": None,
                                  "service_data_off": None}
        self._config(model, {"start_time": "22:00:00",
                             "end_time": "06:00:00"})
        assert model.night_mode is not None
        assert model.night_mode_entity is None

    def test_config_night_entities_join_default_state_entities(self):
        model = _build_model()
        model.light_params_day = {"delay": 180, "service_data": None,
                                  "service_data_off": None}
        model.controlEntities = ["light.downlights"]
        model.stateEntities = ["light.downlights"]
        model._state_entities_explicit = False
        track = self._config(model, {
            "entity": "input_select.house_mode",
            "entity_states": ["noc"],
            "entities": ["light.bed"],
        })
        assert "light.bed" in model.stateEntities
        assert track.called

    def test_config_explicit_state_entities_not_extended(self):
        model = _build_model()
        model.light_params_day = {"delay": 180, "service_data": None,
                                  "service_data_off": None}
        model.controlEntities = ["light.downlights"]
        model.stateEntities = ["sensor.custom"]
        model._state_entities_explicit = True
        track = self._config(model, {
            "entity": "input_select.house_mode",
            "entity_states": ["noc"],
            "entities": ["light.bed"],
        })
        assert model.stateEntities == ["sensor.custom"]
        assert not track.called

    # --- schema -------------------------------------------------------------

    def test_schema_entity_only_valid(self):
        from custom_components.entity_controller import MODE_SCHEMA
        conf = MODE_SCHEMA({
            "entity": "input_select.house_mode",
            "entity_states": ["vecerka", "noc"],
            "entities": ["light.bed"],
        })
        assert conf["entity"] == "input_select.house_mode"

    def test_schema_time_only_valid(self):
        from custom_components.entity_controller import MODE_SCHEMA
        conf = MODE_SCHEMA({"start_time": "22:00:00",
                            "end_time": "06:00:00"})
        assert conf["start_time"] == "22:00:00"

    def test_schema_neither_invalid(self):
        import voluptuous as vol
        from custom_components.entity_controller import MODE_SCHEMA
        with pytest.raises(vol.Invalid):
            MODE_SCHEMA({"delay": 60})


# ---------------------------------------------------------------------------
# 2026-09-19 – link outage of a controlled entity (unavailable round trip)
# ---------------------------------------------------------------------------

class TestLinkOutage:
    """A controlled relay dropping to `unavailable` and returning.

    Real incident 2026-09-18 19:14:56 (light.101_a, localtuya): the relay went
    unavailable for 87 ms while ec_101 was in active_timer. EC treated the
    drop as an external switch-off, went idle and fired a turn_off that the
    offline device never received. That doomed call armed the grace period,
    which then swallowed the relay's `unavailable -> on` return, so the
    resync never ran and the light burned for 39 minutes. Two rules fix it:
    a transition TO unavailable is ignored (the timer keeps running) and the
    resync of a comeback is evaluated BEFORE the grace-period check.
    """

    def _event(self, old_state, new_state, entity_id="light.test"):
        ev = MagicMock()
        ev.data = {
            "entity_id": entity_id,
            "old_state": MagicMock(state=old_state, attributes={}),
            "new_state": MagicMock(
                state=new_state,
                attributes={},
                context=MagicMock(id="device-ctx"),
            ),
        }
        return ev

    def _active_timer_model(self):
        model = _build_model()
        model.is_state_entities_off = MagicMock(return_value=True)
        model.is_state_entities_on = MagicMock(return_value=False)
        model.will_stay_on = MagicMock(return_value=False)
        model.sensor_on()
        assert model.state == "active_timer"
        model.is_ignored_context = MagicMock(return_value=False)
        model.turn_on_control_entities = MagicMock()
        model.turn_off_control_entities = MagicMock()
        model.control = MagicMock()
        return model

    def _idle_model(self):
        model = _build_model()
        assert model.state == "idle"
        model.is_ignored_context = MagicMock(return_value=False)
        model.turn_on_control_entities = MagicMock()
        model.turn_off_control_entities = MagicMock()
        return model

    def _arm_grace(self, model, seconds=5):
        from datetime import timedelta
        model.grace_period = seconds
        model.ignore_state_changes_until = datetime.now() + timedelta(seconds=seconds)
        assert model.is_within_grace_period() is True

    # -- drop to unavailable ------------------------------------------------

    def test_drop_to_unavailable_keeps_timer_running(self):
        """on -> unavailable during active_timer: no transition, no service call."""
        model = self._active_timer_model()
        model._cancel_timer.reset_mock()
        model._start_timer.reset_mock()
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.state_entity_state_change(self._event("on", "unavailable"))
        later.assert_not_called()
        assert model.state == "active_timer", (
            f"Expected active_timer to survive the outage, got {model.state}"
        )
        model._cancel_timer.assert_not_called()
        model._start_timer.assert_not_called()
        model.control.assert_not_called()
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()

    def test_drop_to_unavailable_while_idle_is_ignored(self):
        model = self._idle_model()
        model.state_entity_state_change(self._event("off", "unavailable"))
        assert model.state == "idle"
        model.turn_off_control_entities.assert_not_called()

    def test_drop_to_unavailable_does_not_arm_grace_period(self):
        """The doomed turn_off is gone, so the grace period must not be armed."""
        model = self._active_timer_model()
        model.grace_period = 5
        before = model.ignore_state_changes_until
        model.state_entity_state_change(self._event("on", "unavailable"))
        assert model.ignore_state_changes_until == before

    # -- comeback vs grace period --------------------------------------------

    def test_comeback_on_with_sensor_active_adopts_despite_grace_period(self):
        """unavailable -> on, EC idle, sensor active, grace armed: force_activate."""
        model = self._idle_model()
        self._arm_grace(model)
        model.is_sensor_on = MagicMock(return_value=True)
        model.state_entity_state_change(self._event("unavailable", "on"))
        assert model.state in ("active", "active_timer"), (
            f"Expected adoption of the returned light, got {model.state}"
        )
        model.turn_off_control_entities.assert_not_called()

    def test_comeback_on_with_sensors_quiet_turns_off_despite_grace_period(self):
        """unavailable -> on, EC idle, room quiet, grace armed: clear the stuck light."""
        model = self._idle_model()
        self._arm_grace(model)
        model.is_sensor_on = MagicMock(return_value=False)
        model.state_entity_state_change(self._event("unavailable", "on"))
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    def test_comeback_off_during_timer_turns_on_despite_grace_period(self):
        """unavailable -> off while the timer runs: EC wants it on, turn it on."""
        model = self._active_timer_model()
        self._arm_grace(model)
        model.state_entity_state_change(self._event("unavailable", "off"))
        assert model.state == "active_timer"
        model.turn_on_control_entities.assert_called_once()
        model.control.assert_not_called()

    def test_comeback_on_during_timer_is_left_alone(self):
        model = self._active_timer_model()
        model.state_entity_state_change(self._event("unavailable", "on"))
        assert model.state == "active_timer"
        model.turn_on_control_entities.assert_not_called()
        model.turn_off_control_entities.assert_not_called()
        model.control.assert_not_called()

    def test_comeback_runs_before_ignored_context_check(self):
        """A comeback is never EC's own echo; the context filter must not eat it."""
        model = self._idle_model()
        model.is_ignored_context = MagicMock(return_value=True)
        model.is_sensor_on = MagicMock(return_value=False)
        model.state_entity_state_change(self._event("unavailable", "on"))
        model.turn_off_control_entities.assert_called_once()

    def test_full_incident_replay(self):
        """101_a on 2026-09-18: drop + return 87 ms later with the PIR still on.

        Before the fix EC ended idle with the light on; now the timer survives
        the drop and the comeback is a no-op for a running timer.
        """
        model = self._active_timer_model()
        model.grace_period = 5
        model.is_sensor_on = MagicMock(return_value=True)
        model.state_entity_state_change(self._event("on", "unavailable"))
        model.state_entity_state_change(self._event("unavailable", "on"))
        assert model.state == "active_timer"
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()

    def test_ordinary_external_off_still_handled(self):
        """Regression guard: a real off (not unavailable) still reaches control()."""
        model = self._active_timer_model()
        with patch("custom_components.entity_controller.event.async_call_later") as later:
            model.state_entity_state_change(self._event("on", "off"))
            model.control.assert_not_called()
            model.hass.states.get = MagicMock(return_value=MagicMock(state="off", attributes={}))
            later.call_args[0][2](None)
        model.control.assert_called_once()


# ---------------------------------------------------------------------------
# 2026-10-30 – short state bounce of a controlled entity (state_change_debounce)
# ---------------------------------------------------------------------------

class TestStateChangeDebounce:
    """An external change of a state entity counts only if it persists.

    Real incident 2026-10-30 18:53:58 (light.108_i, localtuya): the relay
    reported on -> off at .558 and off -> on at .646, an 88 ms bounce with a
    device context. ec_108f went active_timer -> blocked 4 ms after the off
    and stayed blocked until 21:07:45, every motion onset renewing the
    7200 s block. Rules: the window starts at the first change and is not
    extended; each entity is compared with its state before the first change
    (an unavailable entity by its last reported state, a removed one is
    skipped); EC acts once; the window is dropped on grace or when EC left
    the state; a blocked re-entry keeps the window.
    """

    E108F = "light.108_f"
    E108I = "light.108_i"
    PROXY = "input_boolean.108_led_proxy_ec"
    ENTITIES = (E108F, E108I, PROXY)
    LATER = "custom_components.entity_controller.event.async_call_later"

    @staticmethod
    def _st(state, **attrs):
        return MagicMock(state=state, attributes=dict(attrs))

    def _set_live(self, model, states):
        """hass.states.get from {entity: state str | State mock | None (removed)}."""
        objs = {e: self._st(s) if isinstance(s, str) else s for e, s in states.items()}
        model.hass.states.get = MagicMock(side_effect=lambda e: objs.get(e))

    def _live(self, model, default, overrides=None):
        states = dict.fromkeys(self.ENTITIES, default)
        states.update(overrides or {})
        self._set_live(model, states)

    def _event(self, entity, old, new, old_attrs=None, new_attrs=None, ctx="device-ctx"):
        ev = MagicMock()
        ev.data = {
            "entity_id": entity,
            "old_state": None if old is None else self._st(old, **(old_attrs or {})),
            "new_state": MagicMock(
                state=new,
                attributes=dict(new_attrs or {}),
                context=MagicMock(id=ctx),
            ),
        }
        return ev

    @staticmethod
    def _fire(later, i=0):
        later.call_args_list[i][0][2](None)

    def _model(self, state="active_timer", debounce=3.0):
        model = _build_model()
        model.stateEntities = list(self.ENTITIES)
        model.controlEntities = list(self.ENTITIES)
        model.turn_on_control_entities = MagicMock()
        model.turn_off_control_entities = MagicMock()
        model.state_change_debounce = debounce
        if state in ("active_timer", "active_stay_on"):
            self._live(model, "off")
            model.stay = state == "active_stay_on"
            model.sensor_on()
            self._live(model, "on")
        elif state == "blocked":
            self._live(model, "off", {self.E108I: "on"})
            model.sensor_on()
        assert model.state == state
        model.turn_on_control_entities.reset_mock()
        model.turn_off_control_entities.reset_mock()
        model._start_timer.reset_mock()
        model._cancel_timer.reset_mock()
        model.hass.services.async_call.reset_mock()
        model.control = MagicMock(side_effect=model.control)
        model.enable = MagicMock(side_effect=model.enable)
        return model

    # -- the incident -------------------------------------------------------

    def test_incident_bounce_keeps_active_timer(self):
        """108_i on -> off -> on inside the window: nothing happens."""
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            assert model.state == "active_timer"
            later.assert_called_once()
            assert later.call_args[0][1] == 3.0
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.turn_off_control_entities.assert_not_called()
        model._start_timer.assert_not_called()
        model._cancel_timer.assert_not_called()

    def test_incident_without_debounce_blocks(self):
        """state_change_debounce 0 is the 9.11.2 path: blocked on the first edge."""
        model = self._model(debounce=0)
        self._live(model, "on", {self.E108I: "off"})
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
        assert model.state == "blocked"
        later.assert_not_called()

    # -- persisted changes ----------------------------------------------------

    def test_persisted_single_off_blocks(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "on", {self.E108I: "off"})
            self._fire(later)
        assert model.state == "blocked"
        model.control.assert_called_once()

    def test_persisted_single_off_with_disable_block_resets_timer(self):
        model = self._model()
        model.disable_block = True
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "on", {self.E108I: "off"})
            self._fire(later)
        assert model.state == "active_timer"
        model._start_timer.assert_called_once()

    def test_persisted_all_off_goes_idle(self):
        model = self._model()
        with patch(self.LATER) as later:
            for e in self.ENTITIES:
                model.state_entity_state_change(self._event(e, "on", "off"))
            self._live(model, "off")
            self._fire(later)
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    # -- one window per burst -------------------------------------------------

    def test_burst_arms_one_window_with_fixed_deadline(self):
        from homeassistant.core import HassJob, HassJobType
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert model.state == "active_timer"
            later.assert_called_once()
            assert later.call_args[0][1] == 3.0
            assert HassJob(later.call_args[0][2]).job_type == HassJobType.Callback
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
            later.assert_called_once()
            self._live(model, "on", {self.E108F: "off"})
            self._fire(later)
        assert model.state == "blocked"
        model.control.assert_called_once()

    def test_sensor_edge_in_window_keeps_check(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.sensor_on()
            assert model.state == "active_timer"
            self._live(model, "on", {self.E108I: "off"})
            self._fire(later)
        assert model.state == "blocked"

    def test_change_after_a_fired_window_opens_a_new_one(self):
        """A fired window frees its slot: the next change is not merged into it."""
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            self._fire(later, 0)
            assert model.state == "active_timer"
            assert model._debounce_window is None
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
            assert later.call_count == 2
            self._live(model, "on", {self.E108F: "off"})
            self._fire(later, 1)
        assert model.state == "blocked"

    def test_blocked_change_after_a_fired_window_opens_a_new_one(self):
        """blocked: bounce dropped, then the owner really switches the last
        light off -> idle, not blocked over a dark room."""
        model = self._model("blocked")
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "off", {self.E108I: "on"})
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            self._fire(later, 0)
            assert model.state == "blocked"
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert later.call_count == 2
            self._fire(later, 1)
        assert model.state == "idle"

    # -- EC leaves active_timer inside the window -----------------------------

    def test_timer_expiry_in_window_drops_check(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.timer_expires()
            assert model.state == "idle"
            assert model._debounce_window is None
            self._fire(later)
        assert model.state == "idle"
        model.control.assert_not_called()

    def test_override_in_window_drops_check(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.override()
            assert model._debounce_window is None
            self._fire(later)
        assert model.state == "overridden"
        model.control.assert_not_called()

    def test_constrain_in_window_drops_check(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.constrain()
            assert model._debounce_window is None
            self._fire(later)
        assert model.state == "constrained"
        model.control.assert_not_called()

    def test_force_activate_reentry_opens_fresh_window(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.force_activate()
            assert model.state == "active_timer"
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
            assert later.call_count == 2
            fresh = model._debounce_window
            assert list(fresh.snapshot) == [self.E108F]
            self._fire(later, 0)
            assert model._debounce_window is fresh
            assert model.state == "active_timer"
            model.control.assert_not_called()
            self._live(model, "on", {self.E108F: "off"})
            self._fire(later, 1)
        assert model.state == "blocked"

    # -- attribute-only changes -----------------------------------------------

    def test_attribute_change_that_persists_calls_control(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 100}, {"brightness": 50}))
            later.assert_called_once()
            self._live(model, "on", {self.E108F: self._st("on", brightness=50)})
            self._fire(later)
        assert model.state == "blocked"

    def test_attribute_change_that_reverts_is_dropped(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 100}, {"brightness": 50}))
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 50}, {"brightness": 100}))
            self._live(model, "on", {self.E108F: self._st("on", brightness=100)})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()

    def test_state_bounce_with_new_attributes_is_dropped(self):
        """Once the state itself changed, only the state is compared (D5)."""
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "off", {"brightness": 100, "color_mode": "hs"}, {}))
            model.state_entity_state_change(self._event(
                self.E108F, "off", "on", {}, {"brightness": 40, "color_mode": "ct"}))
            self._live(model, "on", {
                self.E108F: self._st("on", brightness=40, color_mode="ct")})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()

    def test_state_bounce_then_attribute_report_is_dropped(self):
        """D5: attributes count only if every event of the entity was
        attribute-only, not just the last one."""
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "off", {"brightness": 100}, {}))
            model.state_entity_state_change(self._event(
                self.E108F, "off", "on", {}, {"brightness": 60}))
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 60}, {"brightness": 40}))
            self._live(model, "on", {self.E108F: self._st("on", brightness=40)})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()

    def test_attribute_change_then_state_bounce_is_dropped(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 100}, {"brightness": 50}))
            model.state_entity_state_change(self._event(
                self.E108F, "on", "off", {"brightness": 50}, {}))
            model.state_entity_state_change(self._event(
                self.E108F, "off", "on", {}, {"brightness": 50}))
            self._live(model, "on", {self.E108F: self._st("on", brightness=50)})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()

    # -- what is compared at the deadline -------------------------------------

    def test_unavailable_at_fire_compares_last_known_state(self):
        """A real off followed by a link drop still counts."""
        model = self._model()
        with patch(self.LATER) as later:
            self._live(model, "on", {self.E108I: "off"})
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "on", {self.E108I: "unavailable"})
            model.state_entity_state_change(self._event(self.E108I, "off", "unavailable"))
            self._fire(later)
            assert model.state == "blocked"
            self._live(model, "on", {self.E108I: "off"})
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "off"))
            later.assert_called_once()
        assert model.state == "blocked"
        model.turn_on_control_entities.assert_not_called()

    def test_unavailable_after_bounce_is_not_a_change(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            model.state_entity_state_change(self._event(self.E108I, "on", "unavailable"))
            self._live(model, "on", {self.E108I: "unavailable"})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()

    def test_removed_entity_is_not_compared(self, caplog):
        caplog.set_level(logging.INFO)
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "on", {self.E108I: None})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()
        infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert sum("is gone - not compared" in m for m in infos) == 1
        assert not any("ignoring the bounce" in m for m in infos)

    def test_own_service_call_in_window_drops_check(self, caplog):
        caplog.set_level(logging.INFO)
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.grace_period = 3600
            model.call_service(self.E108F, "turn_on")
            assert model.is_within_grace_period()
            self._live(model, "on", {self.E108I: "off"})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()
        assert any(
            r.levelno == logging.INFO and "(grace period)" in r.getMessage()
            for r in caplog.records
        )

    def test_missing_old_state_counts_as_persisted(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, None, "on"))
            later.assert_called_once()
            self._fire(later)
        assert model.state == "blocked"

    def test_comeback_inside_window_refreshes_last_known(self):
        """108_i bounces off, drops, comes back on, drops again: its last
        report is on, its state before the window, so nothing changed."""
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.state_entity_state_change(self._event(self.E108I, "off", "unavailable"))
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "on"))
            model.state_entity_state_change(self._event(self.E108I, "on", "unavailable"))
            later.assert_called_once()
            self._live(model, "on", {self.E108I: "unavailable"})
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()
        model.turn_on_control_entities.assert_not_called()

    def test_reaction_carries_the_latest_device_context(self):
        model = self._model()
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off", ctx="dev-1"))
            model.state_entity_state_change(self._event(self.E108F, "on", "off", ctx="dev-2"))
            self._live(model, "on", {self.E108I: "off", self.E108F: "off"})
            self._fire(later)
        assert model.state == "blocked"
        assert model.context.parent_id == "dev-2"

    # -- configuration --------------------------------------------------------

    @pytest.mark.parametrize("debounce", [0, None])
    def test_debounce_off_acts_at_once(self, debounce):
        model = self._model(debounce=debounce)
        self._live(model, "on", {self.E108I: "off"})
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.control.assert_called_once()
        later.assert_not_called()

    @pytest.mark.parametrize(
        "cfg, expected, errors",
        [
            ({}, 3.0, 0),
            ({"state_change_debounce": 0}, None, 0),
            ({"state_change_debounce": None}, None, 0),
            ({"state_change_debounce": 1.5}, 1.5, 0),
            ({"state_change_debounce": "5"}, 5.0, 0),
            ({"state_change_debounce": "abc"}, 3.0, 1),
            ({"state_change_debounce": -1}, 3.0, 1),
            ({"state_change_debounce": 600}, 3.0, 1),
            ({"state_change_debounce": float("nan")}, 3.0, 1),
            ({"state_change_debounce": True}, 3.0, 1),
            ({"state_change_debounce": ""}, 3.0, 1),
            ({"state_change_debounce": []}, 3.0, 1),
            ({"state_change_debounce": {}}, 3.0, 1),
            ({"state_change_debounce": 10 ** 400}, 3.0, 1),
        ],
        ids=["default", "zero", "null", "float", "numeric-str", "text",
             "negative", "too-big", "nan", "bool", "empty-str", "list", "dict",
             "overflow"],
    )
    def test_config_parsing(self, caplog, cfg, expected, errors):
        caplog.set_level(logging.INFO)
        model = _build_model()
        model.state_change_debounce = 42.0
        model.config_other(cfg)
        assert model.state_change_debounce == expected
        assert sum(r.levelno == logging.ERROR for r in caplog.records) == errors

    # -- filters still come first ---------------------------------------------

    @pytest.mark.parametrize(
        "case", ["own_context", "grace", "drop", "comeback", "ignored_attribute"])
    def test_filtered_change_arms_nothing(self, case):
        from datetime import timedelta
        model = self._model()
        ev = self._event(self.E108I, "on", "off")
        if case == "own_context":
            ev = self._event(self.E108I, "on", "off", ctx="ec_1a2b3c_0123456789abcdef")
        elif case == "grace":
            model.grace_period = 3600
            model.ignore_state_changes_until = datetime.now() + timedelta(hours=1)
        elif case == "drop":
            ev = self._event(self.E108I, "on", "unavailable")
        elif case == "comeback":
            ev = self._event(self.E108I, "unavailable", "on")
        else:
            model.state_attributes_ignore = ["brightness"]
            ev = self._event(self.E108F, "on", "on", {"brightness": 100}, {"brightness": 50})
        with patch(self.LATER) as later:
            model.state_entity_state_change(ev)
        later.assert_not_called()
        model.control.assert_not_called()
        assert model.state == "active_timer"

    def test_significant_attribute_without_debounce_calls_control(self):
        model = self._model(debounce=0)
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 100}, {"brightness": 50}))
        later.assert_not_called()
        assert model.state == "blocked"

    # -- blocked / active_stay_on: the enable() tail --------------------------

    def test_blocked_bounce_of_last_lit_entity_keeps_light(self):
        """On 9.11.2 the off edge alone sends EC to idle, switching the light off."""
        model = self._model("blocked")
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert model.state == "blocked"
            self._live(model, "off", {self.E108I: "on"})
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            later.assert_called_once()
            self._fire(later)
        assert model.state == "blocked"
        model.enable.assert_not_called()
        model.turn_off_control_entities.assert_not_called()

    def test_blocked_persisted_off_goes_idle_after_window(self):
        model = self._model("blocked")
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert model.state == "blocked"
            model.enable.assert_not_called()
            self._fire(later)
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    def test_blocked_without_debounce_is_synchronous(self):
        model = self._model("blocked", debounce=0)
        self._live(model, "off")
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
        later.assert_not_called()
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    def test_active_stay_on_bounce_keeps_light(self):
        model = self._model("active_stay_on")
        self._live(model, "off", {self.E108I: "on"})
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "off", {self.E108I: "on"})
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            later.assert_called_once()
            self._fire(later)
        assert model.state == "active_stay_on"
        model.enable.assert_not_called()
        model.turn_off_control_entities.assert_not_called()

    def test_active_stay_on_persisted_off_goes_idle(self):
        model = self._model("active_stay_on")
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert model.state == "active_stay_on"
            self._fire(later)
        assert model.state == "idle"

    def test_blocked_sensor_reentry_keeps_check(self):
        """A PIR edge re-enters blocked; the pending check must survive (D18)."""
        model = self._model("blocked")
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.sensor_on()
            assert model.state == "blocked"
            self._fire(later)
        assert model.state == "idle"

    def test_blocked_window_not_applied_after_leaving_blocked(self):
        model = self._model("blocked")
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.block_timer_expires()
            assert model.state == "active_timer"
            self._live(model, "on")
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
            assert later.call_count == 2
            self._fire(later, 0)
            model.enable.assert_not_called()
            assert model.state == "active_timer"
            self._live(model, "on", {self.E108F: "off"})
            self._fire(later, 1)
        assert model.state == "blocked"

    def test_active_stay_on_exit_clears_window(self):
        model = self._model("active_stay_on")
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert model._debounce_window is not None
            model.constrain()
            assert model._debounce_window is None
            self._fire(later)
        assert model.state == "constrained"
        model.enable.assert_not_called()

    def test_blocked_window_cleared_on_idle(self):
        model = self._model("blocked")
        with patch(self.LATER) as later:
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            assert model._debounce_window is not None
            self._live(model, "off")
            model.block_timer_expires()
            assert model.state == "idle"
            assert model._debounce_window is None
            self._fire(later)
        assert model.state == "idle"
        model.enable.assert_not_called()

    @pytest.mark.parametrize(
        "leave, left_to",
        [("block_timer_expires", "active_timer"), ("override", "overridden")])
    def test_blocked_window_still_current_after_leaving_does_nothing(self, leave, left_to):
        """Leaving blocked clears nothing (D18), so the window is still current
        at the fire; the state check must drop it. enable() would raise a
        MachineError from active_timer and would leave overridden."""
        model = self._model("blocked")
        with patch(self.LATER) as later:
            self._live(model, "off", {self.E108I: "on", self.E108F: "on"})
            model.state_entity_state_change(self._event(self.E108F, "off", "on"))
            getattr(model, leave)()
            assert model.state == left_to
            assert model._debounce_window is not None
            self._fire(later)
        assert model.state == left_to
        model.enable.assert_not_called()

    def test_blocked_real_off_then_link_drop_goes_idle(self):
        model = self._model("blocked")
        with patch(self.LATER) as later:
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._live(model, "off", {self.E108I: "unavailable"})
            model.state_entity_state_change(self._event(self.E108I, "off", "unavailable"))
            later.assert_called_once()
            self._fire(later)
            assert model.state == "idle"
            self._live(model, "off")
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "off"))
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    # -- documented limit -----------------------------------------------------

    def test_known_limitation_own_comeback_resync_in_window(self):
        """Known limit R7: an entity returning from unavailable inside the
        window makes EC's resync turn everything on, which arms the grace
        period (ec_108f: 30 s), so the owner's off in the same window is lost.
        """
        model = self._model()
        model.grace_period = 30
        del model.turn_on_control_entities
        with patch(self.LATER) as later:
            self._live(model, "on", {self.E108F: "off"})
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
            later.assert_called_once()
            self._live(model, "on", {self.E108F: "off", self.E108I: "off"})
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "off"))
            assert model.hass.services.async_call.call_count == 3
            self._fire(later)
        assert model.state == "active_timer"
        model.control.assert_not_called()
