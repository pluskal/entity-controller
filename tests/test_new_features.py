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
        model.state_entity_state_change(ev)

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
        model.state_entity_state_change(ev)

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
        model.state_entity_state_change(ev)

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
