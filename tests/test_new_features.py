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
    hass.is_stopping = False
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
        source=["pending", "idle", "blocked", "constrained", "overridden", "active_timer"],
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
        m.block_on_external_on = True
        m._block_ending = False
        m._block_anchor = m._block_timer_started = 0.0
        m.block_timeout = None
        m.grace_period = None
        m.ignore_state_changes_until = datetime.now()
        m.homeassistant_turn_on_domains = ["group"]
        m._expiry_time = None
        m._restore_wait = None

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
    TRACK = "custom_components.entity_controller.event.async_track_state_change_event"

    def _restore_model(self, saved, live):
        """A model still in pending (startup) with `saved` in its store.

        `live` maps entity -> state (None = not created yet) and is read on
        every lookup, so a test can change it later. The motion sensor
        reports 'off' unless `live` says otherwise."""
        live.setdefault("binary_sensor.motion", "off")
        model = _build_model()
        model.to_pending()
        model.context = None  # production: nothing sets it before the restore
        model.stateEntities = ["light.a", "light.b"]
        model.controlEntities = ["light.a", "light.b"]
        model.block_timeout = 600
        model.turn_on_control_entities = MagicMock()
        model.turn_off_control_entities = MagicMock()
        model.hass.states.get = MagicMock(
            side_effect=lambda e: MagicMock(state=live[e]) if live.get(e) is not None else None)
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
        """Run the restore; returns (result, Timer mock, async_call_later mock).
        The state-listener mock is kept in self.track."""
        with patch("custom_components.entity_controller.Timer") as timer, patch(
            self.NO_SAVE, side_effect=lambda coro, loop: coro.close()
        ), patch(self.LATER) as later, patch(self.TRACK) as track:
            result = asyncio.run(model._async_restore_state())
        self.track = track
        return result, timer, later

    def _later(self, fn, *args):
        """Run fn (a wait event or timeout, a trigger) with Timer, saves and
        listeners patched; returns the Timer mock."""
        with patch("custom_components.entity_controller.Timer") as timer, patch(
            self.NO_SAVE, side_effect=lambda coro, loop: coro.close()
        ), patch(self.LATER), patch(self.TRACK):
            fn(*args)
        return timer

    @staticmethod
    def _ev(entity, old, new):
        ev = MagicMock()
        ev.data = {"entity_id": entity,
                   "old_state": None if old is None else MagicMock(state=old),
                   "new_state": None if new is None else MagicMock(state=new)}
        return ev

    def _waiting_model(self, expires_in, lights, sensor="on", overrides=()):
        """A restore that waits in pending; returns (model, live, expiry, later)."""
        live = {"binary_sensor.motion": sensor, **lights}
        model, expiry = self._timer_restore_model(expires_in, live)
        model.overrideEntities = list(overrides)
        del model._start_timer
        result, timer, later = self._restore(model)
        assert result is True and model.state == "pending"
        assert model._restore_wait is not None
        timer.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        model.turn_off_control_entities.assert_not_called()
        return model, live, expiry, later

    def _resumed_for_a_silent_sensor(self):
        live = {"light.a": "on", "light.b": "on", "binary_sensor.motion": "unknown"}
        model, _ = self._timer_restore_model(-600, live)
        model.sensor_type = "duration"
        model.config["sensor_resets_timer"] = True
        del model._start_timer
        del model.turn_off_control_entities
        result, _, _ = self._restore(model)
        assert result is True and model.state == "active_timer"
        return model, live

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

    def test_async_restore_timer_with_time_left_resumes_it(self, caplog):
        """A running timer resumes with the time it had left and the lights
        untouched. No ERROR log: finalize() takes the trigger's resume=...
        argument; transitions only logs a finalize that does not."""
        caplog.set_level(logging.ERROR)
        model, expiry = self._timer_restore_model(150, {"light.a": "on", "light.b": "off"})
        del model._start_timer  # the real timer bookkeeping, Timer patched
        model.entity.do_update.reset_mock()
        result, timer, later = self._restore(model)
        assert result is True
        assert model.state == "active_timer"
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        timer.assert_called_once()
        delay, expire_cb = timer.call_args[0]
        assert 140 < delay <= 150  # the time left, not the configured 180 s
        assert expire_cb == model.timer_expire
        timer.return_value.start.assert_called_once()
        assert abs((model._expiry_time - expiry).total_seconds()) < 10
        assert model._restore_wait is None
        later.assert_not_called()
        self.track.assert_not_called()
        assert model.context.id.startswith("ec_")
        assert model.entity.do_update.called
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

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

    @pytest.mark.parametrize("missing", ["unavailable", "unknown", None])
    def test_async_restore_timer_resumes_with_an_unreachable_light(self, missing):
        """A light that has not reported yet no longer sends EC through idle,
        which switched the reachable lights off (9.9.1)."""
        model, _ = self._timer_restore_model(150, {"light.a": missing, "light.b": "on"})
        del model._start_timer
        result, timer, later = self._restore(model)
        assert result is True and model.state == "active_timer"
        model.turn_off_control_entities.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        assert 140 < timer.call_args[0][0] <= 150
        assert model._restore_wait is None
        later.assert_not_called()
        self.track.assert_not_called()

    @pytest.mark.parametrize("on_enter_idle", ["off", "ignore", "on"])
    def test_async_restore_expired_timer_turns_off_once(self, on_enter_idle):
        """Ran out while HA was down, every sensor off: each light is switched
        off exactly once (9.11.3: twice), whatever on_enter_idle says."""
        model, _ = self._timer_restore_model(-60, {"light.a": "on", "light.b": "on"})
        model.transition_behaviours["on_enter_idle"] = on_enter_idle
        result, timer, _ = self._restore(model)
        assert result is True and model.state == "idle"
        model.turn_off_control_entities.assert_called_once()
        model.turn_on_control_entities.assert_not_called()
        timer.assert_not_called()
        assert model.context.id.startswith("ec_")

    def test_async_restore_timer_with_lights_off_goes_idle(self):
        """Unchanged: nothing left to finish."""
        model, _ = self._timer_restore_model(3600, {"light.a": "off", "light.b": "off"})
        result, timer, later = self._restore(model)
        assert result is False
        assert model.state == "pending"
        later.assert_not_called()
        timer.assert_not_called()
        self.track.assert_not_called()

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

    def test_async_restore_expired_timer_with_a_light_unreachable_turns_off_once(self):
        model, _ = self._timer_restore_model(-60, {"light.a": "on", "light.b": "unavailable"})
        result, timer, _ = self._restore(model)
        assert result is True and model.state == "idle"
        model.turn_off_control_entities.assert_called_once()
        timer.assert_not_called()
        self.track.assert_not_called()

    @pytest.mark.parametrize("holder", ["sensor", "hold"])
    def test_async_restore_expired_timer_held_by_a_sensor_resumes_the_delay(self, holder):
        """The timer ran out while a sensor or hold sensor held the light
        ('pending sensor'): resume the delay, lights untouched (9.11.3 switched
        the occupied room dark)."""
        live = {"light.a": "on", "light.b": "on",
                "binary_sensor.motion": "on" if holder == "sensor" else "off",
                "binary_sensor.hold": "on" if holder == "hold" else "off"}
        model, _ = self._timer_restore_model(-600, live)
        model.holdSensorEntities = ["binary_sensor.hold"]
        del model._start_timer
        result, timer, _ = self._restore(model)
        assert result is True and model.state == "active_timer"
        model.turn_off_control_entities.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        assert timer.call_args[0][0] == 180

    @pytest.mark.parametrize("motion,hold", [("unknown", "off"), ("unavailable", "off"), (None, "off"),
                                             ("off", "unknown")],
                             ids=["unknown", "unavailable", "None", "hold-unknown"])
    def test_async_restore_expired_timer_with_a_silent_sensor_resumes_the_delay(self, motion, hold, caplog):
        """Also: a sensor not created yet is 'not reported', without the
        'does not exist (yet)' ERROR of is_sensor_on(); a hold sensor that
        has not reported holds the light like a regular one."""
        caplog.set_level(logging.ERROR)
        model, _ = self._timer_restore_model(
            -600, {"light.a": "on", "light.b": "on", "binary_sensor.motion": motion, "binary_sensor.hold": hold})
        model.holdSensorEntities = ["binary_sensor.hold"]
        del model._start_timer
        result, timer, _ = self._restore(model)
        assert result is True and model.state == "active_timer"
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        assert timer.call_args[0][0] == 180
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    @pytest.mark.parametrize("night,expected", [(True, 240), (False, 60)], ids=["night", "day"])
    def test_async_restore_expired_timer_resumes_the_delay_of_the_mode(self, night, expected):
        model, _ = self._timer_restore_model(
            -600, {"light.a": "on", "light.b": "on", "binary_sensor.motion": "unknown"})
        model.light_params_day = {"delay": 60, "service_data": None, "service_data_off": None}
        model.light_params_night = {"delay": 240, "service_data": None, "service_data_off": None}
        model.night_mode = {"delay": 240}
        model.is_night = MagicMock(return_value=night)
        del model._start_timer
        result, timer, _ = self._restore(model)
        assert result is True
        assert timer.call_args[0][0] == expected

    def test_resumed_delay_is_held_by_a_late_sensor_report(self):
        model, live = self._resumed_for_a_silent_sensor()
        live["binary_sensor.motion"] = "on"
        timer = self._later(model.sensor_state_change, self._ev("binary_sensor.motion", "unknown", "on"))
        timer.assert_called_once()  # the timer restarts
        self._later(model.timer_expire)
        assert model.state == "active_timer"  # 'pending sensor'
        model.hass.services.async_call.assert_not_called()

    def test_resumed_delay_runs_out_without_a_sensor_report(self):
        model, _ = self._resumed_for_a_silent_sensor()
        self._later(model.timer_expire)
        assert model.state == "idle"
        calls = model.hass.services.async_call.call_args_list
        assert [c.args[1] for c in calls] == ["turn_off", "turn_off"]
        assert all(c.kwargs["context"].id.startswith("ec_") for c in calls)

    @pytest.mark.parametrize("sensor,low,high", [("unknown", 180, 180), ("off", 5, 10), ("on", 5, 10)])
    def test_async_restore_timer_with_little_time_left(self, sensor, low, high):
        model, _ = self._timer_restore_model(
            10, {"light.a": "on", "light.b": "on", "binary_sensor.motion": sensor})
        del model._start_timer
        result, timer, _ = self._restore(model)
        assert result is True and model.state == "active_timer"
        assert low <= timer.call_args[0][0] <= high

    @pytest.mark.parametrize("day,night,backoff,expected", [
        (180, None, None, 180), (60, 240, None, 240), (60, None, 600, 600), (60, 240, False, 240),
    ], ids=["day", "night-counts-by-day", "backoff-max", "backoff-off"])
    def test_async_restore_caps_the_resumed_time(self, day, night, backoff, expected):
        """A saved run-out an hour ahead (DST fall-back, clock step) resumes at
        most the longest timer the instance can run."""
        model, _ = self._timer_restore_model(3650, {"light.a": "on", "light.b": "on"})
        model.light_params_day = {"delay": day, "service_data": None, "service_data_off": None}
        if night is not None:
            model.night_mode = {"delay": night}
            model.light_params_night = {"delay": night, "service_data": None, "service_data_off": None}
        if backoff is not None:
            model.backoff = bool(backoff)
            model.backoff_max = backoff or 7200
        model.is_night = MagicMock(return_value=False)
        del model._start_timer
        result, timer, _ = self._restore(model)
        assert result is True
        assert timer.call_args[0][0] == expected

    def test_async_restore_with_no_light_reachable_waits_in_pending(self):
        model, _, expiry, later = self._waiting_model(150, {"light.a": "unavailable", "light.b": "unknown"})
        assert model._restore_wait.expiry == expiry
        hass, entities, job = self.track.call_args[0]
        assert hass is model.hass and entities == ["light.a", "light.b"]
        assert job.func == model._restore_wait_event and job.args == (model._restore_wait,)
        later.assert_called_once()
        assert later.call_args[0][:2] == (model.hass, 300)

    def test_async_restore_with_the_reachable_lights_off_waits(self):
        self._waiting_model(150, {"light.a": "off", "light.b": "unavailable"})

    @pytest.mark.parametrize("old", ["unavailable", "unknown", None])
    def test_wait_resumes_when_a_light_comes_back_on(self, old):
        model, live, _, later = self._waiting_model(150, {"light.a": old, "light.b": "unavailable"})
        wait, unsub = model._restore_wait, self.track.return_value
        live["light.a"] = "on"
        timer = self._later(model._restore_wait_event, wait, self._ev("light.a", old, "on"))
        assert model.state == "active_timer"
        assert 140 < timer.call_args[0][0] <= 150
        model.hass.services.async_call.assert_not_called()
        assert model._restore_wait is None
        unsub.assert_called_once()
        later.return_value.assert_called_once()

    @pytest.mark.parametrize("sensor", ["off", "on"])
    def test_wait_applies_the_restore_rules_at_the_comeback(self, sensor):
        model, live, _, _ = self._waiting_model(
            -60, {"light.a": "unavailable", "light.b": "unavailable"}, sensor=sensor)
        live["light.a"] = "on"
        timer = self._later(model._restore_wait_event, model._restore_wait,
                            self._ev("light.a", "unavailable", "on"))
        if sensor == "off":
            assert model.state == "idle"
            model.turn_off_control_entities.assert_called_once()
            timer.assert_not_called()
        else:
            assert model.state == "active_timer"
            model.turn_off_control_entities.assert_not_called()
            assert timer.call_args[0][0] == 180

    @pytest.mark.parametrize("debounce", [3.0, None], ids=["settled", "no-debounce"])
    def test_wait_ends_in_idle_without_turn_off_when_the_lights_come_back_off(self, debounce):
        """Once every light has stayed off for state_change_debounce (none: at once)."""
        model, live, _, _ = self._waiting_model(
            150, {"light.a": "unavailable", "light.b": "unavailable"}, sensor="off")
        model.state_change_debounce = debounce
        wait = model._restore_wait
        live["light.a"] = "off"
        self._later(model._restore_wait_event, wait, self._ev("light.a", "unavailable", "off"))
        assert model.state == "pending" and model._restore_wait is wait  # light.b is still unavailable
        live["light.b"] = "off"
        self._later(model._restore_wait_event, wait, self._ev("light.b", "unavailable", "off"))
        if debounce:
            assert model.state == "pending" and model._restore_wait is wait
            self._later(model._restore_wait_settled, wait)
        assert model.state == "idle" and model._restore_wait is None
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()

    @pytest.mark.parametrize("old,new", [("on", "unavailable"), ("unavailable", "unknown"), ("unavailable", None),
                                         ("off", "on")],
                             ids=["drop", "still-unknown", "removed", "manual-on"])
    def test_wait_ignores_changes_that_are_not_a_comeback(self, old, new):
        """Also a reachable light switched on by hand: evaluated after the
        run-out with every sensor off, it would be switched off again."""
        manual = old == "off"
        model, live, _, _ = self._waiting_model(
            -60 if manual else 150,
            {"light.a": "off" if manual else "unavailable", "light.b": "unavailable"},
            sensor="off" if manual else "on")
        wait = model._restore_wait
        live["light.a" if manual else "light.b"] = "on"  # would settle the timer if the event were evaluated
        self._later(model._restore_wait_event, wait, self._ev("light.a", old, new))
        assert model.state == "pending" and model._restore_wait is wait
        model.turn_off_control_entities.assert_not_called()

    @pytest.mark.parametrize("then,sensor,optout", [
        ("comeback", "off", None), ("override-attribute", "off", None), ("comeback", "on", None),
        ("comeback", "off", "block_on_external_on"), ("comeback", "off", "disable_block"),
    ], ids=["comeback", "override-attribute", "comeback-sensor-on", "opt-out", "disable-block"])
    def test_wait_never_switches_off_a_light_switched_on_during_it(self, then, sensor, optout):
        """A light switched on during the wait (by hand, an automation) is
        external control: the next evaluation ends the wait in blocked
        without a turn-off, as in idle at runtime (9.12.0; 9.11.4 ended it in
        idle). With block_on_external_on false or disable_block, idle."""
        model, live, _, _ = self._waiting_model(
            -60, {"light.a": "off", "light.b": "unavailable", "input_boolean.ov": "off"}, sensor=sensor,
            overrides=["input_boolean.ov"])
        if optout == "block_on_external_on":
            model.block_on_external_on = False
        elif optout == "disable_block":
            model.disable_block = True
        wait = model._restore_wait
        live["light.a"] = "on"
        self._later(model._restore_wait_event, wait, self._ev("light.a", "off", "on"))
        assert model.state == "pending" and model._restore_wait is wait
        if then == "comeback":
            live["light.b"] = "off"
            timer = self._later(model._restore_wait_event, wait, self._ev("light.b", "unavailable", "off"))
        else:
            timer = self._later(model._restore_wait_event, wait, self._ev("input_boolean.ov", "off", "off"))
        assert model._restore_wait is None
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        if optout:
            assert model.state == "idle"
            timer.assert_not_called()
        else:
            assert model.state == "blocked"
            timer.assert_called_once_with(600, model.block_timer_expire)

    @pytest.mark.parametrize("case", ["off", "unknown", "late"])
    def test_wait_settles_a_light_that_comes_back_off_then_on(self, case):
        """Tasmota lights come back 'off' and report their real state 1-2 s
        later. Within state_change_debounce of its comeback the turn on
        settles the timer by the restore rules; later it is someone
        switching the light on, which the restore never switches off: the
        wait ends in blocked (9.12.0)."""
        model, live, _, _ = self._waiting_model(
            -60, {"light.a": "unavailable", "light.b": "off"}, sensor="unknown" if case == "unknown" else "off")
        wait = model._restore_wait
        live["light.a"] = "off"  # placeholder
        self._later(model._restore_wait_event, wait, self._ev("light.a", "unavailable", "off"))
        assert model.state == "pending" and model._restore_wait is wait
        if case == "late":
            wait.returned["light.a"] -= model.state_change_debounce + 1  # came back long ago
        live["light.a"] = "on"  # the real state
        timer = self._later(model._restore_wait_event, wait, self._ev("light.a", "off", "on"))
        if case == "late":
            assert model.state == "pending"
            timer = self._later(model._restore_wait_settled, wait)
            assert model.state == "blocked"
            timer.assert_called_once_with(600, model.block_timer_expire)
            model.turn_off_control_entities.assert_not_called()
        elif case == "off":  # ran out, every sensor reports off
            assert model.state == "idle"
            model.turn_off_control_entities.assert_called_once()
        else:  # a sensor has not reported: the delay
            assert model.state == "active_timer"
            assert timer.call_args[0][0] == 180
            model.turn_off_control_entities.assert_not_called()
        assert model._restore_wait is None

    def test_wait_reads_a_missing_override_without_an_error(self, caplog):
        """An override entity not created yet reads as off, without the
        'does not exist (yet)' ERROR at the restore and at each comeback."""
        caplog.set_level(logging.ERROR)
        model, live, _, _ = self._waiting_model(
            150, {"light.a": "unavailable", "light.b": "unavailable"}, overrides=["input_boolean.missing"])
        live["light.a"] = "on"
        self._later(model._restore_wait_event, model._restore_wait, self._ev("light.a", "unavailable", "on"))
        assert model.state == "active_timer"
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_wait_gives_up_after_300_s_without_turn_off(self):
        model, _, _, _ = self._waiting_model(
            150, {"light.a": "unavailable", "light.b": "unavailable"}, sensor="off")
        wait = model._restore_wait
        self._later(model._restore_wait_timeout, wait, None)
        assert model.state == "idle" and model._restore_wait is None
        model.turn_off_control_entities.assert_not_called()
        self._later(model._restore_wait_timeout, wait, None)  # a stale timeout does nothing
        assert model.state == "idle"

    @pytest.mark.parametrize("case", ["block", "opt-out", "switched-back-off"])
    def test_wait_bound_blocks_a_light_switched_on_during_it(self, case, caplog):
        """The 5-minute bound ends the wait in blocked when a light switched
        on during it is still on (9.12.0; 9.11.4 left it on in idle)."""
        caplog.set_level(logging.WARNING)
        model, live, _, _ = self._waiting_model(
            150, {"light.a": "off", "light.b": "unavailable"}, sensor="off")
        if case == "opt-out":
            model.block_on_external_on = False
        wait = model._restore_wait
        live["light.a"] = "on"
        self._later(model._restore_wait_event, wait, self._ev("light.a", "off", "on"))
        if case == "switched-back-off":
            live["light.a"] = "off"
            self._later(model._restore_wait_event, wait, self._ev("light.a", "on", "off"))
        assert model.state == "pending" and model._restore_wait is wait
        timer = self._later(model._restore_wait_timeout, wait, None)
        assert model._restore_wait is None
        model.turn_off_control_entities.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        if case == "block":
            assert model.state == "blocked"
            timer.assert_called_once_with(600, model.block_timer_expire)
        else:
            assert model.state == "idle"
            timer.assert_not_called()
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and "ending the wait without a turn-off" in warnings[0]

    def test_override_that_turns_on_ends_the_wait(self):
        model, live, _, _ = self._waiting_model(
            150, {"light.a": "unavailable", "light.b": "unavailable", "input_boolean.ov": "off"},
            overrides=["input_boolean.ov"])
        assert self.track.call_args[0][1] == ["light.a", "light.b", "input_boolean.ov"]
        live["input_boolean.ov"] = "on"
        self._later(model._restore_wait_event, model._restore_wait, self._ev("input_boolean.ov", "off", "on"))
        assert model.state == "overridden" and model._restore_wait is None
        model.turn_off_control_entities.assert_not_called()

    def test_constrain_ends_the_wait(self):
        model, live, _, later = self._waiting_model(150, {"light.a": "unavailable", "light.b": "unavailable"})
        wait = model._restore_wait
        self._later(model.constrain)
        assert model.state == "constrained" and model._restore_wait is None
        self.track.return_value.assert_called_once()
        later.return_value.assert_called_once()
        live["light.a"] = "on"
        self._later(model._restore_wait_event, wait, self._ev("light.a", "unavailable", "on"))
        assert model.state == "constrained"

    def test_wait_does_nothing_while_ha_stops_and_is_saved_as_the_timer(self):
        model, live, expiry, _ = self._waiting_model(150, {"light.a": "unavailable", "light.b": "unavailable"})
        model.hass.is_stopping = True
        live["light.a"] = "on"
        self._later(model._restore_wait_event, model._restore_wait, self._ev("light.a", "unavailable", "on"))
        assert model.state == "pending"
        asyncio.run(model._async_save_state())
        saved = model._store.async_save.call_args[0][0]
        assert saved["state"] == "active_timer" and saved["expires_at"] == str(expiry)

    def test_forced_sensor_during_the_wait_activates(self):
        model, _, _, _ = self._waiting_model(150, {"light.a": "unavailable", "light.b": "off"})
        self._later(model.force_activate)
        assert model.state == "active_timer" and model._restore_wait is None
        model.turn_on_control_entities.assert_called_once()

    @pytest.mark.parametrize("saved_state", ["idle", "blocked", "active_timer"])
    def test_restore_leaves_ec_alone_once_it_left_pending(self, saved_state):
        """A forced sensor (or constrain) moves EC out of pending while the
        store loads: the restore returns True and does nothing, so the caller's
        start_monitoring() or restore_blocked() cannot raise MachineError."""
        from datetime import timedelta
        saved = {"state": saved_state, "saved_at": "x",
                 "expires_at": str(datetime.now() + timedelta(seconds=150))}
        model = self._restore_model(saved, {"light.a": "on", "light.b": "on"})

        async def load():
            model.force_activate()  # a forced sensor's on-edge during the await
            return saved
        model._store.async_load = load
        result, timer, later = self._restore(model)
        assert result is True and model.state == "active_timer"
        model.turn_on_control_entities.assert_called_once()  # force_activate's own
        model.turn_off_control_entities.assert_not_called()
        later.assert_not_called()
        self.track.assert_not_called()

    def test_async_restore_expired_timer_in_stay_mode_stays_on(self):
        model, _ = self._timer_restore_model(
            -600, {"light.a": "on", "light.b": "on", "binary_sensor.motion": "on"})
        model.stay = True
        del model._start_timer
        result, _, _ = self._restore(model)
        assert result is True and model.state == "active_stay_on"
        model.hass.services.async_call.assert_not_called()

    @pytest.mark.parametrize("end,holder,lux", [
        ("lights-back-off", "sensor", None), ("bound", "sensor", None), ("bound", "hold", None),
        ("bound", "sensor", "500"),
    ], ids=["lights-back-off-sensor", "bound-sensor", "bound-hold", "bound-sensor-bright"])
    def test_wait_ending_in_idle_acts_on_a_sensor_that_is_on(self, end, holder, lux):
        """A sensor edge in pending is dropped; when the wait ends in idle, a
        regular sensor that is on activates EC as its edge would, lux-gated
        with the same re-check. A hold sensor never switches the lights on."""
        model, live, _, _ = self._waiting_model(
            150, {"light.a": "unavailable", "light.b": "unavailable"}, sensor="off")
        model.holdSensorEntities = ["binary_sensor.hold"]
        if lux is not None:
            model.luxEntity, model.lux_threshold = "sensor.lux", 60
            live["sensor.lux"] = lux
        live["binary_sensor.motion" if holder == "sensor" else "binary_sensor.hold"] = "on"
        wait = model._restore_wait
        if end == "bound":
            self._later(model._restore_wait_timeout, wait, None)
        else:
            for e in ("light.a", "light.b"):
                live[e] = "off"
                self._later(model._restore_wait_event, wait, self._ev(e, "unavailable", "off"))
            self._later(model._restore_wait_settled, wait)
        assert model._restore_wait is None
        model.turn_off_control_entities.assert_not_called()
        if lux is not None:  # too bright: idle, with the re-check of a late reading armed
            assert model.state == "idle" and model._lux_blocked
            model.turn_on_control_entities.assert_not_called()
            assert model._lux_recheck_handle is not None
            assert model._lux_recheck_entity == "binary_sensor.motion"
        elif holder == "sensor":
            assert model.state == "active_timer"
            model.turn_on_control_entities.assert_called_once()
        else:
            assert model.state == "idle"
            model.turn_on_control_entities.assert_not_called()

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

    # -- idle: switched on outside EC (9.12.0) --------------------------------

    MOTION = "binary_sensor.motion"
    TIMER = "custom_components.entity_controller.Timer"
    NO_SAVE = "custom_components.entity_controller.asyncio.run_coroutine_threadsafe"

    def _idle_live(self, model, on=(), sensor="off", unavailable=()):
        states = dict.fromkeys(self.ENTITIES, "off")
        states.update(dict.fromkeys(on, "on"))
        states.update(dict.fromkeys(unavailable, "unavailable"))
        states[self.MOTION] = sensor
        self._set_live(model, states)

    def _idle(self, debounce=3.0):
        """idle, every light off, block_timeout 1800; the blocked trigger is
        wrapped so a test can tell B apart from other ways into blocked."""
        model = self._model("idle", debounce=debounce)
        model.block_timeout = 1800
        model.sensor_type = "duration"
        model.blocked = MagicMock(side_effect=model.blocked)
        self._idle_live(model)
        return model

    @staticmethod
    def _attrs(model):
        return {c.args[0]: c.args[1] for c in model.entity.set_attr.call_args_list}

    @pytest.mark.parametrize("old", ["off", "unknown", None])
    def test_idle_external_on_blocks(self, old, caplog):
        caplog.set_level(logging.INFO)
        model = self._idle()
        model.entity.set_attr.reset_mock()
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, old, "on"))
            assert model.state == "idle"
            later.assert_called_once()
            assert later.call_args[0][1] == 3.0
            model.blocked.assert_not_called()
            self._fire(later)
        assert model.state == "blocked"
        model.blocked.assert_called_once()
        timer.assert_called_once_with(1800, model.block_timer_expire)
        timer.return_value.start.assert_called_once()
        attrs = self._attrs(model)
        assert attrs["blocked_by"] == self.E108I
        assert attrs["last_triggered_by"] == self.E108I
        assert attrs["notes"] == "Switched on outside EC while idle"
        assert isinstance(attrs["expires_at"], datetime)
        model.hass.services.async_call.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.turn_off_control_entities.assert_not_called()
        assert model.context.parent_id == "device-ctx"
        infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
        assert sum("switched on outside EC while idle" in m for m in infos) == 1
        assert not any("changed for good" in m for m in infos)

    @pytest.mark.parametrize("debounce", [0, None])
    def test_idle_external_on_without_debounce_blocks_at_once(self, debounce):
        model = self._idle(debounce=debounce)
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
        assert model.state == "blocked"
        later.assert_not_called()
        model.blocked.assert_called_once()
        timer.assert_called_once_with(1800, model.block_timer_expire)
        model.hass.services.async_call.assert_not_called()

    @pytest.mark.parametrize("other", ["other-off", "other-on"])
    def test_idle_second_change_joins_the_window(self, other):
        """Once an ON edge opened the window, any change joins it."""
        model = self._idle()
        with patch(self.LATER) as later, patch(self.TIMER):
            if other == "other-off":
                self._idle_live(model, on=[self.E108F])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            if other == "other-off":
                model.state_entity_state_change(self._event(self.E108F, "on", "off"))
                self._idle_live(model, on=[self.E108I])
            else:
                model.state_entity_state_change(self._event(self.E108F, "off", "on"))
                self._idle_live(model, on=[self.E108I, self.E108F])
            later.assert_called_once()
            assert set(model._debounce_window.snapshot) == {self.E108I, self.E108F}
            self._fire(later)
        assert model.state == "blocked"
        model.blocked.assert_called_once()
        model.turn_off_control_entities.assert_not_called()

    @pytest.mark.parametrize("case", ["bounce", "removed"])
    def test_idle_bounce_stays_idle(self, case, caplog):
        """An ON edge that does not persist (back off, or the entity removed)
        stays idle and logs at DEBUG only: in idle only a take-over logs at INFO."""
        caplog.set_level(logging.DEBUG)
        model = self._idle()
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            if case == "bounce":
                model.state_entity_state_change(self._event(self.E108I, "on", "off"))
                self._idle_live(model)
            else:
                self._set_live(model, {self.E108I: None, self.E108F: "off", self.PROXY: "off",
                                       self.MOTION: "off"})
            later.assert_called_once()
            self._fire(later)
        assert model.state == "idle"
        model.blocked.assert_not_called()
        timer.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        model.turn_off_control_entities.assert_not_called()
        assert not [r.getMessage() for r in caplog.records
                    if r.levelno >= logging.INFO and r.name == model.log.name]
        expected = "is back to its previous state" if case == "bounce" else "is gone - not compared"
        assert any(r.levelno == logging.DEBUG and expected in r.getMessage() for r in caplog.records)

    def test_idle_only_an_off_persisted_stays_idle(self, caplog):
        """The ON edge bounced back; another light that went off for good
        joined the window. Nothing was switched on, so nothing blocks."""
        caplog.set_level(logging.INFO)
        model = self._idle()
        self._idle_live(model, on=[self.E108F])
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
            later.assert_called_once()
            self._idle_live(model)
            self._fire(later)
        assert model.state == "idle"
        model.blocked.assert_not_called()
        timer.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        assert not [r.getMessage() for r in caplog.records
                    if r.levelno >= logging.INFO and r.name == model.log.name]

    @pytest.mark.parametrize("case", ["own_context", "ignored_source"])
    def test_idle_filtered_on_arms_nothing(self, case):
        model = self._idle()
        if case == "own_context":
            ev = self._event(self.E108I, "off", "on", ctx="ec_1a2b3c_0123456789abcdef")
        else:
            model.ignored_event_sources = ["01JABCDEF"]
            ev = self._event(self.E108I, "off", "on", ctx="01JABCDEF")
        self._idle_live(model, on=[self.E108I])
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            model.state_entity_state_change(ev)
        later.assert_not_called()
        timer.assert_not_called()
        model.blocked.assert_not_called()
        assert model.state == "idle"

    @pytest.mark.parametrize("case", ["relit", "back-off", "ec-call-meanwhile", "clock-back"])
    def test_idle_on_inside_grace_is_judged_after_it(self, case, caplog):
        """An ON edge inside the grace period (EC's own echo in idle is an
        off) opens a window that ends max(debounce, 1 s) after the grace
        period, at most grace_period + that after the edge. clock-back pins
        only that cap: after a real wall-clock step back the fire's own grace
        check still drops the window (README known limit)."""
        from datetime import timedelta
        caplog.set_level(logging.DEBUG)
        model = self._idle()
        model.grace_period = 5
        ahead = 3600 if case == "clock-back" else 4
        model.ignore_state_changes_until = datetime.now() + timedelta(seconds=ahead)
        with patch(self.LATER) as later, patch(self.TIMER):
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            later.assert_called_once()
            delay = later.call_args[0][1]
            if case == "clock-back":
                assert delay == 5 + 3.0
            else:
                assert 4 + 3.0 - 0.5 < delay <= 4 + 3.0
            assert any("acting only if it persists for %.1f s" % delay in r.getMessage()
                       for r in caplog.records)
            if case == "back-off":
                model.state_entity_state_change(self._event(self.E108I, "on", "off"))
                self._idle_live(model)
            later.assert_called_once()
            if case == "ec-call-meanwhile":
                model.ignore_state_changes_until = datetime.now() + timedelta(seconds=5)
            else:
                model.ignore_state_changes_until = datetime.now() - timedelta(seconds=1)
            self._fire(later)
        expected = "blocked" if case in ("relit", "clock-back") else "idle"
        assert model.state == expected
        assert model.blocked.call_count == (1 if expected == "blocked" else 0)
        model.hass.services.async_call.assert_not_called()

    def test_block_end_by_manual_off_then_relight_blocks(self):
        """A B-block ended by switching the light off: on_enter_idle's own
        turn_off arms the grace period, and a re-light 2 s later is judged
        once it ends (9.12.0 design probe P5: it used to stay on in idle)."""
        from datetime import timedelta
        model = self._idle()
        model.grace_period = 5
        model.turn_off_control_entities = MagicMock(
            side_effect=lambda: model.call_service(self.E108I, "turn_off"))
        with patch(self.LATER) as later, patch(self.TIMER), patch(
                self.NO_SAVE, side_effect=lambda coro, loop: coro.close()):
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            self._fire(later, 0)
            assert model.state == "blocked"
            self._idle_live(model)
            model.state_entity_state_change(self._event(self.E108I, "on", "off"))
            self._fire(later, 1)
            assert model.state == "idle"
            model.turn_off_control_entities.assert_called_once()
            assert model.is_within_grace_period()
            model.ignore_state_changes_until -= timedelta(seconds=2)  # the re-light, 2 s later
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            assert later.call_count == 3
            assert 3 + 3.0 - 0.5 < later.call_args[0][1] <= 3 + 3.0
            model.ignore_state_changes_until = datetime.now() - timedelta(seconds=1)
            self._fire(later, 2)
        assert model.state == "blocked"
        assert model.blocked.call_count == 2

    @pytest.mark.parametrize("sensor", ["quiet", "sensor-on"])
    def test_idle_comeback_on_opens_no_window(self, sensor):
        """unavailable -> on keeps the resync of TestLinkOutage."""
        model = self._idle()
        self._idle_live(model, on=[self.E108I], sensor="on" if sensor == "sensor-on" else "off")
        with patch(self.LATER) as later, patch(self.TIMER):
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "on"))
        later.assert_not_called()
        model.blocked.assert_not_called()
        if sensor == "quiet":
            assert model.state == "idle"
            model.turn_off_control_entities.assert_called_once()
        else:
            assert model.state == "active_timer"
            model.turn_on_control_entities.assert_called_once()
            model.turn_off_control_entities.assert_not_called()

    @pytest.mark.parametrize("case", ["significant", "ignored"])
    def test_idle_attribute_change_arms_nothing(self, case):
        model = self._idle()
        if case == "ignored":
            model.state_attributes_ignore = ["brightness"]
        self._idle_live(model, on=[self.E108F])
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            model.state_entity_state_change(self._event(
                self.E108F, "on", "on", {"brightness": 100}, {"brightness": 50}))
        later.assert_not_called()
        timer.assert_not_called()
        model.blocked.assert_not_called()
        assert model.state == "idle"
        assert model._debounce_window is None

    def test_idle_off_edge_arms_nothing(self):
        model = self._idle()
        self._idle_live(model, on=[self.E108I])
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            model.state_entity_state_change(self._event(self.E108F, "on", "off"))
        later.assert_not_called()
        timer.assert_not_called()
        assert model.state == "idle"
        assert model._debounce_window is None
        model.turn_off_control_entities.assert_not_called()

    @pytest.mark.parametrize("debounce", [3.0, 0], ids=["debounce", "no-debounce"])
    @pytest.mark.parametrize("optout", ["block_on_external_on", "disable_block"])
    def test_idle_external_on_left_alone(self, optout, debounce):
        model = self._idle(debounce=debounce)
        if optout == "block_on_external_on":
            model.block_on_external_on = False
        else:
            model.disable_block = True
        self._idle_live(model, on=[self.E108I])
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
        later.assert_not_called()
        timer.assert_not_called()
        model.blocked.assert_not_called()
        assert model.state == "idle"
        model.hass.services.async_call.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.turn_off_control_entities.assert_not_called()

    @pytest.mark.parametrize("state", ["pending", "constrained", "overridden", "active_timer", "active_stay_on"])
    def test_external_on_outside_idle_unchanged(self, state):
        """An external ON edge does what it did before 9.12.0 outside idle."""
        if state in ("active_timer", "active_stay_on"):
            model = self._model(state)
        else:
            model = self._model("idle")
            {"pending": model.to_pending, "constrained": model.constrain,
             "overridden": model.override}[state]()
        model.block_timeout = 1800
        model.blocked = MagicMock(side_effect=model.blocked)
        self._live(model, "on", {self.MOTION: "off"})
        with patch(self.LATER) as later, patch(self.TIMER):
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            if state in ("pending", "constrained", "overridden"):
                later.assert_not_called()
            else:
                later.assert_called_once()
                assert model._debounce_window.state == state
                self._fire(later)
        model.blocked.assert_not_called()
        if state == "active_timer":
            model.control.assert_called_once()
            assert model.state == "blocked"
        elif state == "active_stay_on":
            model.enable.assert_called_once()
            assert model.state == "active_stay_on"
        else:
            assert model.state == state
            model.control.assert_not_called()
            model.enable.assert_not_called()

    @pytest.mark.parametrize("leave", ["sensor_on", "override", "constrain", "force_activate", "back-to-idle"])
    def test_idle_window_dropped_when_ec_leaves_idle(self, leave):
        model = self._idle()
        with patch(self.LATER) as later, patch(self.TIMER):
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            if leave == "back-to-idle":
                model.constrain()
                model.enable()
                assert model.state == "idle" and model._debounce_window is None
            else:
                getattr(model, leave)()
            left_to = model.state
            self._fire(later)
        assert model.state == left_to
        model.blocked.assert_not_called()

    def test_idle_reboot_off_then_on_blocks(self):
        """A rebooting relay returns 'off', then reports 'on': B (D12)."""
        model = self._idle()
        with patch(self.LATER) as later, patch(self.TIMER):
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "off"))
            later.assert_not_called()
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            self._fire(later)
        assert model.state == "blocked"
        model.turn_off_control_entities.assert_not_called()

    def test_known_limitation_comeback_via_unknown_blocks(self):
        """Known limit: unavailable -> unknown -> on is a B, while
        unavailable -> on is the comeback resync."""
        model = self._idle()
        with patch(self.LATER) as later, patch(self.TIMER):
            model.state_entity_state_change(self._event(self.E108I, "unavailable", "unknown"))
            later.assert_not_called()
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "unknown", "on"))
            self._fire(later)
        assert model.state == "blocked"
        model.turn_off_control_entities.assert_not_called()

    def test_known_limitation_unavailable_at_deadline_stays_idle(self):
        """Known limit: the only lit light is unavailable at the deadline, so
        no state entity reads on; its comeback is then resynced."""
        model = self._idle()
        with patch(self.LATER) as later, patch(self.TIMER) as timer:
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            model.state_entity_state_change(self._event(self.E108I, "on", "unavailable"))
            self._idle_live(model, unavailable=[self.E108I])
            self._fire(later)
        assert model.state == "idle"
        model.blocked.assert_not_called()
        timer.assert_not_called()

    def test_idle_external_on_block_is_saved(self):
        """B10: a B-block is saved as blocked (restored by
        test_async_restore_blocked_with_a_light_on_leaves_it_on)."""
        model = self._idle()
        model._store = MagicMock()
        model._store.async_save = AsyncMock()
        with patch(self.LATER) as later, patch(self.TIMER), patch(self.NO_SAVE) as schedule:
            self._idle_live(model, on=[self.E108I])
            model.state_entity_state_change(self._event(self.E108I, "off", "on"))
            self._fire(later)
        assert model.state == "blocked"
        schedule.assert_called_once()
        coro, loop = schedule.call_args.args
        assert loop is model.hass.loop
        asyncio.run(coro)
        assert model._store.async_save.call_args[0][0]["state"] == "blocked"

    @pytest.mark.parametrize("cfg, expected, errors", [
        ({}, True, 0),
        ({"block_on_external_on": False}, False, 0),
        ({"block_on_external_on": True}, True, 0),
        ({"block_on_external_on": "false"}, True, 1),
        ({"block_on_external_on": None}, True, 1),
        ({"block_on_external_on": 0}, True, 1),
        ({"block_on_external_on": 1}, True, 1),
        ({"block_on_external_on": "yes"}, True, 1),
    ], ids=["default", "false", "true", "str-false", "null", "zero", "one", "str-yes"])
    def test_block_on_external_on_parsing(self, caplog, cfg, expected, errors):
        caplog.set_level(logging.INFO)
        model = _build_model()
        model.config_other(cfg)
        assert model.block_on_external_on is expected
        assert sum(r.levelno == logging.ERROR for r in caplog.records) == errors
        infos = [r for r in caplog.records
                 if r.levelno == logging.INFO and "block_on_external_on is false" in r.getMessage()]
        assert len(infos) == (0 if expected else 1)
        model.entity.set_attr.assert_any_call("block_on_external_on", expected)


# ---------------------------------------------------------------------------
# 2026-11-10 – a block taken over in idle: hold sensors, expiry, turn-off
# ---------------------------------------------------------------------------

class TestBlockedHoldSensors:
    """In blocked, a hold sensor's ON edge restarts the block timer in place,
    at most once per 60 s and within hold_max_seconds of the block start or
    the last regular-sensor edge; the expiry rules after a take-over; a stale
    block timer; the turn-off at the end of a block that ends in idle."""

    L1, L2 = "light.a", "light.b"
    W, BED, READING = "light.205_wardrobe", "light.205_bed", "light.205_reading"
    PIR, RADAR = "binary_sensor.pir", "binary_sensor.radar"
    LATER = "custom_components.entity_controller.event.async_call_later"
    TIMER = "custom_components.entity_controller.Timer"
    NO_SAVE = "custom_components.entity_controller.asyncio.run_coroutine_threadsafe"

    def _timer(self):
        """Timer patched; every Timer(...) is a fresh mock."""
        return patch(self.TIMER, side_effect=lambda *a, **k: MagicMock())

    def _no_save(self):
        return patch(self.NO_SAVE, side_effect=lambda coro, loop: coro.close())

    @staticmethod
    def _event(entity, old, new, ctx="device-ctx"):
        ev = MagicMock()
        ev.data = {
            "entity_id": entity,
            "old_state": None if old is None else MagicMock(state=old, attributes={}),
            "new_state": MagicMock(state=new, attributes={}, context=MagicMock(id=ctx)),
        }
        return ev

    def _get(self, entity):
        s = self.live.get(entity)
        if s is None:
            return None
        return MagicMock(state=s, attributes={}, last_changed=self.changed.get(entity, self.now))

    def _model(self, control=(L1, L2), night=(), extra_state=(), sensor_type="duration"):
        from homeassistant.util import dt as dt_util
        model = _build_model()
        model.controlEntities = list(control)
        model.nightControlEntities = list(night)
        model.stateEntities = list(control) + list(night) + list(extra_state)
        model.activeControlEntities = list(control)
        model.sensor_type = sensor_type
        model.sensorEntities = [self.PIR]
        model.holdSensorEntities = [self.RADAR]
        model.hold_max_seconds = 7200
        model.block_timeout = 1800
        model.turn_on_control_entities = MagicMock()
        model.turn_off_control_entities = MagicMock()
        self.now = dt_util.utcnow()
        self.changed = {}
        self.live = dict.fromkeys(model.stateEntities + [self.PIR, self.RADAR], "off")
        model.hass.states.get = MagicMock(side_effect=self._get)
        return model

    def _take_over(self, model, light):
        """idle -> blocked through an external ON and its debounce window."""
        with patch(self.LATER) as later:
            self.live[light] = "on"
            model.state_entity_state_change(self._event(light, "off", "on"))
            later.call_args[0][2](None)
        assert model.state == "blocked"

    def _blocked(self, sensor_type="duration"):
        model = self._model(sensor_type=sensor_type)
        self._take_over(model, self.L1)
        return model

    @staticmethod
    def _service_targets(model, service):
        return [c.args[2]["entity_id"] for c in model.hass.services.async_call.call_args_list
                if c.args[1] == service]

    # -- hold sensors in blocked ----------------------------------------------

    def test_hold_on_edge_restarts_block_timer_in_place(self):
        from unittest.mock import call
        with self._timer() as timer:
            model = self._blocked()
            old = model.block_timer_handle
            model._block_timer_started -= 61
            model.do_transition_behaviour = MagicMock(side_effect=model.do_transition_behaviour)
            model._schedule_save_state = MagicMock()
            model.entity.set_attr.reset_mock()
            self.live[self.RADAR] = "on"
            model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
        assert model.state == "blocked"
        old.cancel.assert_called_once()
        assert timer.call_args_list == [call(1800, model.block_timer_expire)] * 2
        assert model.block_timer_handle is not old
        model.block_timer_handle.start.assert_called_once()
        attrs = {c.args[0]: c.args[1] for c in model.entity.set_attr.call_args_list}
        assert isinstance(attrs["expires_at"], datetime)
        assert attrs["notes"] == "Hold sensor binary_sensor.radar is on, block timer restarted"
        assert "last_triggered_by" not in attrs and "blocked_at" not in attrs
        model.do_transition_behaviour.assert_not_called()
        model._schedule_save_state.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.turn_off_control_entities.assert_not_called()

    def test_hold_edges_are_throttled(self):
        """A flapping radar restarts the block timer at most once a minute."""
        with self._timer() as timer:
            model = self._blocked()
            self.live[self.RADAR] = "on"
            ev = self._event(self.RADAR, "off", "on")
            for _ in range(100):
                model.hold_sensor_state_change(ev)
            assert timer.call_count == 1  # only the block start
            model._block_timer_started -= 61
            for _ in range(100):
                model.hold_sensor_state_change(ev)
        assert timer.call_count == 2
        assert model.state == "blocked"

    @pytest.mark.parametrize("case", ["no-regular-edge", "regular-edge-resets", "cap-null"])
    def test_hold_rearm_bounded_by_hold_max_seconds(self, case, caplog):
        """A hold sensor keeps a block only within hold_max_seconds of the
        block start or the last regular-sensor edge (null: no bound)."""
        caplog.set_level(logging.DEBUG)
        with self._timer() as timer:
            model = self._blocked()
            if case == "cap-null":
                model.hold_max_seconds = None
            model._block_timer_started -= 61
            model._block_anchor -= 7300
            self.live[self.RADAR] = "on"
            ev = self._event(self.RADAR, "off", "on")
            model.hold_sensor_state_change(ev)
            restarts = timer.call_count - 1
            if case == "regular-edge-resets":
                self.live[self.PIR] = "on"
                model.sensor_state_change(self._event(self.PIR, "off", "on"))
                assert model.state == "blocked" and timer.call_count == 2  # re-entry
                model._block_timer_started -= 61
                model.hold_sensor_state_change(ev)
                assert timer.call_count == 3
        if case == "cap-null":
            assert restarts == 1
        else:
            assert restarts == 0
            assert any("the block runs out" in r.getMessage() for r in caplog.records)

    @pytest.mark.parametrize("state", ["idle", "constrained", "overridden", "pending"])
    def test_hold_on_edge_outside_blocked_does_nothing(self, state):
        with self._timer() as timer:
            model = self._model()
            self.live[self.L1] = "on"
            if state != "idle":
                {"constrained": model.constrain, "overridden": model.override,
                 "pending": model.to_pending}[state]()
            self.live[self.RADAR] = "on"
            model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
        assert model.state == state
        timer.assert_not_called()
        model.hass.services.async_call.assert_not_called()
        model.turn_on_control_entities.assert_not_called()
        model.turn_off_control_entities.assert_not_called()

    def test_hold_off_in_blocked_does_nothing(self):
        with self._timer() as timer:
            model = self._blocked()
            model._block_timer_started -= 61
            model._schedule_save_state = MagicMock()
            self.live[self.RADAR] = "off"
            model.hold_sensor_state_change(self._event(self.RADAR, "on", "off"))
        assert model.state == "blocked"
        assert timer.call_count == 1
        model.block_timer_handle.cancel.assert_not_called()
        model._schedule_save_state.assert_not_called()
        model.hass.services.async_call.assert_not_called()

    def test_hold_rules_in_active_timer_unchanged(self):
        model = self._model()
        model.sensor_on()
        assert model.state == "active_timer"
        model._cancel_timer.reset_mock()
        model._start_timer.reset_mock()
        model.entity.set_attr.reset_mock()
        self.live[self.RADAR] = "on"
        model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
        model._cancel_timer.assert_called_once()
        model._start_timer.assert_not_called()
        model.entity.set_attr.assert_any_call("expires_at", "pending sensor")
        self.live[self.RADAR] = "off"
        model.hold_sensor_state_change(self._event(self.RADAR, "on", "off"))
        model._start_timer.assert_called_once()
        assert model.state == "active_timer"

    def test_blocked_hold_edge_keeps_check(self):
        """A hold restart is no re-entry: a pending check of blocked survives it."""
        with self._timer(), patch(self.LATER) as later:
            model = self._blocked()
            self.live[self.L1] = "off"
            model.state_entity_state_change(self._event(self.L1, "on", "off"))
            window = model._debounce_window
            assert window is not None and window.state == "blocked"
            model._block_timer_started -= 61
            self.live[self.RADAR] = "on"
            model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
            assert model._debounce_window is window and model.state == "blocked"
            later.call_args[0][2](None)
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    # -- the block timer ---------------------------------------------------------

    @pytest.mark.parametrize("case", ["idle-after-expiry", "replaced-handle"])
    def test_stale_block_timer_does_nothing(self, case):
        current = "custom_components.entity_controller.current_thread"
        with self._timer():
            model = self._blocked()
            if case == "idle-after-expiry":
                model.block_timer_expires()
                assert model.state == "idle"
                model.turn_off_control_entities.reset_mock()
                model.block_timer_expire()  # a timer that was not cancelled; MachineError before 9.12.0
                assert model.state == "idle"
                model.turn_off_control_entities.assert_not_called()
            else:
                handle = model.block_timer_handle
                handle.function = model.block_timer_expire
                with patch(current, return_value=MagicMock(function=model.block_timer_expire)):
                    model.block_timer_expire()  # a replaced timer's thread
                assert model.state == "blocked"
                model.turn_off_control_entities.assert_not_called()
                with patch(current, return_value=handle):
                    model.block_timer_expire()  # the current timer's thread
                assert model.state == "idle"
                model.turn_off_control_entities.assert_called_once()

    def test_real_timer_stale_guard(self):
        """Real threading.Timer threads, each joined before its asserts: a
        stray timer with the same function is ignored, the timer replaced by
        a hold restart never runs, and the current one ends the block."""
        import threading
        model = self._model()
        model.block_timeout = 60  # long: must not run out during the test
        timers = []
        try:
            self._take_over(model, self.L1)
            first = model.block_timer_handle
            timers.append(first)
            assert isinstance(first, threading.Timer) and first.is_alive()
            stray = threading.Timer(0.01, model.block_timer_expire)
            timers.append(stray)
            stray.start()
            stray.join(10)
            assert not stray.is_alive() and model.state == "blocked"
            model.block_timeout = 0.2
            model._block_timer_started -= 61
            # the radar reads off again by the time the new timer runs out
            model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
            second = model.block_timer_handle
            timers.append(second)
            assert second is not first
            first.join(10)
            assert not first.is_alive()  # cancelled
            second.join(10)
            assert not second.is_alive()
        finally:
            for t in timers:
                t.cancel()
                t.join(10)
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()
        assert model.block_timer_handle is None

    def test_no_block_timeout_take_over(self):
        """block_timeout unset: B still blocks, without a timer; a hold edge
        does nothing; switching the light off ends the block as usual."""
        with self._timer() as timer, patch(self.LATER) as later:
            model = self._model()
            model.block_timeout = None
            self.live[self.L1] = "on"
            model.state_entity_state_change(self._event(self.L1, "off", "on"))
            later.call_args[0][2](None)
            assert model.state == "blocked"
            timer.assert_not_called()
            self.live[self.RADAR] = "on"
            model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
            timer.assert_not_called()
            self.live[self.L1] = "off"
            model.state_entity_state_change(self._event(self.L1, "on", "off"))
            later.call_args[0][2](None)
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()

    def test_block_expiry_racing_a_hold_restart_raises_nothing(self):
        """The expiring timer's worker leaves blocked (on_exit_blocked clears
        the handle) while a hold restart is between setting the new handle
        and starting it: no AttributeError ('NoneType' has no 'start').
        Deterministic interleave: the restart's loop.time() read runs the
        worker's transition."""
        created = []

        def make_timer(*_args, **_kwargs):
            created.append(MagicMock())
            return created[-1]

        with patch(self.TIMER, side_effect=make_timer):
            model = self._blocked()
            model._block_timer_started -= 61
            real_time, reads = model.hass.loop.time, []

            def time_with_expiry():
                reads.append(None)
                if len(reads) == 2:  # in _start_block_timer, after the new handle is set
                    model.block_timer_expires()  # the worker, past the stale guard
                return real_time()

            self.live[self.RADAR] = "on"
            with patch.object(model.hass.loop, "time", side_effect=time_with_expiry):
                model.hold_sensor_state_change(self._event(self.RADAR, "off", "on"))
        assert len(reads) == 2
        assert model.state == "active_timer"  # the radar holds at the expiry: adopted
        first, second = created
        first.cancel.assert_called()
        second.cancel.assert_called_once()  # by the worker's on_exit_blocked
        second.start.assert_called_once()  # a cancelled Timer's thread runs nothing
        assert model.block_timer_handle is None

    def test_external_on_block_expiry_quiet_turns_off_once(self):
        with self._timer():
            model = self._blocked()
            model.block_timer_expire()
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()
        model.turn_on_control_entities.assert_not_called()

    @pytest.mark.parametrize("holder", ["sensor", "hold-fresh"])
    def test_external_on_block_expiry_held_adopts(self, holder):
        with self._timer():
            model = self._blocked()
            self.live[self.PIR if holder == "sensor" else self.RADAR] = "on"
            model.block_timer_expire()
        assert model.state == "active_timer"
        model.turn_on_control_entities.assert_called_once()
        model.turn_off_control_entities.assert_not_called()

    def test_external_on_block_expiry_adopts_the_whole_mode_set(self):
        """Adopting switches on every control entity of the mode, not only
        the one switched on outside EC (existing behaviour, README)."""
        with self._timer(), self._no_save():
            model = self._blocked()
            del model.turn_on_control_entities
            self.live[self.PIR] = "on"
            model.hass.services.async_call.reset_mock()
            model.block_timer_expire()
        assert model.state == "active_timer"
        assert sorted(self._service_targets(model, "turn_on")) == [self.L1, self.L2]

    def test_external_on_block_expiry_with_stuck_hold_turns_off(self, caplog):
        from datetime import timedelta
        caplog.set_level(logging.WARNING)
        with self._timer():
            model = self._blocked()
            self.live[self.RADAR] = "on"
            self.changed[self.RADAR] = self.now - timedelta(seconds=7300)
            model.block_timer_expire()
        assert model.state == "idle"
        model.turn_off_control_entities.assert_called_once()
        model.turn_on_control_entities.assert_not_called()
        assert any("treating it as stuck" in r.getMessage() for r in caplog.records)

    def test_external_on_block_expiry_event_sensor_adopts(self):
        with self._timer():
            model = self._blocked(sensor_type="event")
            model.block_timer_expire()
        assert model.state == "active_timer"
        model.turn_on_control_entities.assert_called_once()
        model.turn_off_control_entities.assert_not_called()

    # -- the turn-off at the end of a block -----------------------------------

    @pytest.mark.parametrize("active, switched, expected", [
        ([W], BED, [W, BED]), ([BED], W, [BED, W]),
    ], ids=["day-set-bed-lit", "night-set-main-lit"])
    def test_block_end_turns_off_lit_targets_of_both_modes(self, active, switched, expected):
        """ec_205: a block that ends in idle also switches off a lit target
        of the other mode; an explicit state entity is never a target."""
        with self._timer(), self._no_save():
            model = self._model(control=[self.W], night=[self.BED], extra_state=[self.READING])
            model.activeControlEntities = list(active)
            del model.turn_off_control_entities
            self.live[self.READING] = "on"
            self._take_over(model, switched)
            model.hass.services.async_call.reset_mock()
            model.block_timer_expire()
        assert model.state == "idle"
        assert self._service_targets(model, "turn_off") == expected
        assert model._block_ending is False

    @pytest.mark.parametrize("case", ["adoption", "override-off"])
    def test_block_end_flag_is_cleared(self, case):
        """Only a turn-off in the trigger that ends the block in idle is
        widened: not a later timer run-out, not an override's own turn-off."""
        with self._timer(), self._no_save():
            model = self._model(control=[self.W], night=[self.BED])
            model.activeControlEntities = [self.W]
            del model.turn_off_control_entities
            self._take_over(model, self.BED)
            model.hass.services.async_call.reset_mock()
            if case == "adoption":
                self.live[self.PIR] = "on"
                model.block_timer_expire()
                assert model.state == "active_timer" and model._block_ending is False
                self.live[self.PIR] = "off"
                model.timer_expires()
                assert model.state == "idle"
            else:
                model.transition_behaviours["on_enter_overridden"] = "off"
                model.override()
                assert model.state == "overridden"
        assert self._service_targets(model, "turn_off") == [self.W]
        assert model._block_ending is False

    def test_start_time_during_external_on_block_stays_blocked(self, caplog):
        """Pinned: start_time inside a block logs the existing WARNING."""
        caplog.set_level(logging.WARNING)
        with self._timer():
            model = self._blocked()
            model._apply_start_time_transition()
        assert model.state == "blocked"
        assert any("no transition from state 'blocked'" in r.getMessage() for r in caplog.records)
