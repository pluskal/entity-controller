"""
This file is part of Entity Controller.

Entity Controller is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Entity Controller is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with Entity Controller.  If not, see <https://www.gnu.org/licenses/>.

"""
"""
Entity controller component for Home Assistant.
Maintainer:       Daniel Mason
Version:          v9.7.6
Project Page:     https://danielbkr.net/projects/entity-controller/
Documentation:    https://github.com/danobot/entity-controller
"""
import asyncio
import functools
import hashlib
import logging
from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.util import dt as dt_util
import re
from datetime import date, datetime, time, timedelta
from threading import Timer, current_thread
import pprint
from typing import Optional, List

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from homeassistant.const import CONF_NAME, SUN_EVENT_SUNRISE, SUN_EVENT_SUNSET
from homeassistant.core import callback, Context
from homeassistant.helpers import entity, event, service
from homeassistant.helpers.template import Template
from homeassistant.helpers.entity_component import EntityComponent
from homeassistant.helpers.sun import get_astral_event_date
from homeassistant.util import dt
import homeassistant.util.yaml.objects as YamlObjects
import homeassistant.util.uuid as uuid_util
from transitions import Machine
from transitions.core import MachineError
from transitions.extensions import HierarchicalMachine as Machine
from homeassistant.helpers.service import async_call_from_config

DEPENDENCIES = ["light", "sensor", "binary_sensor", "cover", "fan", "media_player"]
from homeassistant.helpers.storage import Store
from homeassistant.const import EVENT_HOMEASSISTANT_STOP

from .const import (
    DOMAIN,
    DOMAIN_SHORT,
    STATES,

    CONF_START_TIME,
    CONF_END_TIME,
    CONF_TRANSITION_BEHAVIOUR_ON,
    CONF_TRANSITION_BEHAVIOUR_OFF,
    CONF_TRANSITION_BEHAVIOUR_IGNORE,

    # Behaviours
    CONF_BEHAVIOURS,
    CONF_ON_ENTER_IDLE,
    CONF_ON_EXIT_IDLE,
    CONF_ON_ENTER_ACTIVE,
    CONF_ON_EXIT_ACTIVE,
    CONF_ON_ENTER_OVERRIDDEN,
    CONF_ON_EXIT_OVERRIDDEN,
    CONF_ON_ENTER_CONSTRAINED,
    CONF_ON_EXIT_CONSTRAINED,
    CONF_ON_ENTER_BLOCKED,
    CONF_ON_EXIT_BLOCKED,

    SENSOR_TYPE_DURATION,
    SENSOR_TYPE_EVENT,
    MODE_DAY,
    MODE_NIGHT,
    DEFAULT_DELAY,
    DEFAULT_BRIGHTNESS,
    DEFAULT_NAME,
    CONF_CONTROL_ENTITIES,
    CONF_CONTROL_ENTITY,
    CONF_TRIGGER_ON_ACTIVATE,
    CONF_TRIGGER_ON_DEACTIVATE,
    CONF_SENSOR,
    CONF_SENSORS,
    CONF_SERVICE_DATA,
    CONF_SERVICE_DATA_OFF,
    CONF_STATE_ENTITIES,
    CONF_DELAY,
    CONF_BLOCK_TIMEOUT,
    CONF_DISABLE_BLOCK,
    CONF_BLOCK_ON_EXTERNAL_ON,
    CONF_SENSOR_TYPE_DURATION,
    CONF_SENSOR_TYPE,
    CONF_SENSOR_RESETS_TIMER,
    CONF_NIGHT_MODE,
    CONF_NIGHT_MODE_ENTITY,
    CONF_NIGHT_MODE_ENTITY_STATES,
    CONF_NIGHT_MODE_ENTITIES,
    CONF_STATE_ATTRIBUTES_IGNORE,
    CONF_IGNORED_EVENT_SOURCES,
    CONSTRAIN_START,
    CONSTRAIN_END,

    CONF_IGNORE_STATE_CHANGES_UNTIL,

    CONTEXT_ID_CHARACTER_LIMIT,

    # New features
    CONF_FORCED_SENSORS,
    CONF_HOLD_SENSORS,
    CONF_HOLD_MAX_SECONDS,
    CONF_EVENT_SENSORS,
    CONF_EVENT_SENSOR_TYPE,
    CONF_LUX_ENTITY,
    CONF_LUX_THRESHOLD,
    CONF_LUX_BRIGHT_STATES,
    CONF_LUX_RECHECK_DELAY,
    CONF_STATE_CHANGE_DEBOUNCE,
    DEFAULT_STATE_CHANGE_DEBOUNCE,
    STORAGE_VERSION,
    STORAGE_KEY_PREFIX,
)

from .entity_services import (
    async_setup_entity_services,
)



VERSION = '9.8.0'


_LOGGER = logging.getLogger(__name__)

# Configure delay before starting to monitor state change events
STARTUP_DELAY = 70

# Longest a restored running timer waits in pending for its lights when no
# reachable one is on (9.11.4); EC then starts in idle (in blocked for a light
# switched on during the wait) without switching anything off.
RESTORE_WAIT_SECONDS = 300

# A hold sensor restarts the block timer at most once per this many seconds
# (2026-11-10; the 103 radar flips about 54 times a day).
HOLD_REARM_THROTTLE = 60

devices = []
MODE_SCHEMA = vol.Schema(
    vol.All(
        {
            # Default must be None because we differentiate between set and
            # unset. Guarded by vol.Any: current voluptuous validates inserted
            # defaults, and Coerce(dict) on None always fails.
            vol.Optional(CONF_SERVICE_DATA, default=None): vol.Any(
                None, vol.Coerce(dict)
            ),
            vol.Optional(CONF_SERVICE_DATA_OFF, default=None): vol.Any(
                None, vol.Coerce(dict)
            ),
            vol.Optional(CONF_START_TIME): cv.string,
            vol.Optional(CONF_END_TIME): cv.string,
            vol.Optional(CONF_NIGHT_MODE_ENTITY): cv.entity_id,
            vol.Optional(CONF_NIGHT_MODE_ENTITY_STATES, default=[]): vol.All(
                cv.ensure_list, [cv.string]
            ),
            vol.Optional(CONF_NIGHT_MODE_ENTITIES, default=[]): cv.entity_ids,
            vol.Optional(CONF_DELAY, default=DEFAULT_DELAY): cv.positive_int,
        },
        # night detection needs a time window, a state entity, or both
        cv.has_at_least_one_key(CONF_NIGHT_MODE_ENTITY, CONF_START_TIME),
    )
)

ENTITY_SCHEMA = vol.Schema(
    cv.has_at_least_one_key(
        CONF_CONTROL_ENTITIES, CONF_CONTROL_ENTITY, CONF_TRIGGER_ON_ACTIVATE
    ),
    {
        # vol.Required(CONF_NAME): cv.string,
        vol.Optional(CONF_DELAY, default=DEFAULT_DELAY): cv.positive_int,
        vol.Optional(CONF_START_TIME): cv.string,
        vol.Optional(CONF_END_TIME): cv.string,
        vol.Optional(CONF_SENSOR_TYPE_DURATION, default=False): cv.boolean,
        vol.Optional(CONF_SENSOR_TYPE, default=SENSOR_TYPE_EVENT): vol.All(
            vol.Lower, vol.Any(SENSOR_TYPE_EVENT, SENSOR_TYPE_DURATION)
        ),
        vol.Optional(CONF_SENSOR_RESETS_TIMER, default=False): cv.boolean,
        vol.Optional(CONF_SENSOR, default=[]): cv.entity_ids,
        vol.Optional(CONF_SENSORS, default=[]): cv.entity_ids,
        vol.Optional(CONF_CONTROL_ENTITIES, default=[]): cv.entity_ids,
        vol.Optional(CONF_CONTROL_ENTITY, default=[]): cv.entity_ids,
        vol.Optional(CONF_TRIGGER_ON_ACTIVATE, default=None): cv.entity_ids,
        vol.Optional(CONF_TRIGGER_ON_DEACTIVATE, default=None): cv.entity_ids,
        vol.Optional(CONF_STATE_ENTITIES, default=[]): cv.entity_ids,
        vol.Optional(CONF_BLOCK_TIMEOUT, default=None): cv.positive_int,
        vol.Optional(CONF_DISABLE_BLOCK, default=False): cv.boolean,
        # documentation only; runtime accepts a real bool (config_other)
        vol.Optional(CONF_BLOCK_ON_EXTERNAL_ON, default=True): cv.boolean,
        vol.Optional(CONF_IGNORE_STATE_CHANGES_UNTIL, default=None): vol.Any(None, cv.positive_int),
        vol.Optional(CONF_NIGHT_MODE, default=None): MODE_SCHEMA,
        vol.Optional(CONF_STATE_ATTRIBUTES_IGNORE, default=[]): cv.ensure_list,
        vol.Optional(CONF_IGNORED_EVENT_SOURCES, default=[]): cv.ensure_list,
        vol.Optional(CONF_FORCED_SENSORS, default=[]): cv.entity_ids,
        vol.Optional(CONF_HOLD_SENSORS, default=[]): cv.entity_ids,
        vol.Optional(CONF_HOLD_MAX_SECONDS, default=7200): vol.Any(
            cv.positive_int, None
        ),
        vol.Optional(CONF_EVENT_SENSORS, default=[]): vol.All(cv.ensure_list, [cv.string]),
        vol.Optional(CONF_LUX_ENTITY, default=None): vol.Any(None, cv.entity_id),
        vol.Optional(CONF_LUX_THRESHOLD, default=None): vol.Any(None, vol.Coerce(float)),
        vol.Optional(CONF_LUX_BRIGHT_STATES, default=[]): vol.All(cv.ensure_list, [cv.string]),
        vol.Optional(CONF_LUX_RECHECK_DELAY, default=1.0): vol.Any(None, vol.Coerce(float)),
        vol.Optional(CONF_STATE_CHANGE_DEBOUNCE, default=DEFAULT_STATE_CHANGE_DEBOUNCE): vol.Any(
            None, vol.All(vol.Coerce(float), vol.Range(min=0, max=60))
        ),
        vol.Optional(CONF_SERVICE_DATA, default=None): vol.Coerce(
            dict
        ),
        vol.Optional(CONF_BEHAVIOURS, default=None): vol.Coerce(
            dict
        ),
        # Default must be none because we differentiate between set and unset
        vol.Optional(CONF_SERVICE_DATA_OFF, default=None): vol.Coerce(dict),
    },
    # extra=vol.ALLOW_EXTRA,
)

PLATFORM_SCHEMA = cv.schema_with_slug_keys(ENTITY_SCHEMA)


async def async_setup(hass, config):
    """Load graph configurations."""

    if(str(((datetime.now()).astimezone()).tzinfo) != str(dt.as_local(dt.now()).tzname())):
        _LOGGER.error("Timezones do not Match. Mismatched timezones may cause unintended behaviours.")
        _LOGGER.error("System DateTime: %s", ((datetime.now()).astimezone()).tzinfo )
        _LOGGER.error("Home Assistant DateTime: %s", dt.as_local(dt.now()).tzname())

    component = EntityComponent(_LOGGER, DOMAIN, hass)

    _LOGGER.info(
        "If you have ANY issues with EntityController (v"
        + VERSION
        + "), please enable DEBUG logging under the logger component and kindly report the issue on Github. https://github.com/danobot/entity-controller/issues"
    )

    async_setup_entity_services(component)

    machine = Machine(
        states=STATES,
        initial="pending",
        # title=self.name+" State Diagram",
        # show_conditions=True
        # show_auto_transitions = True,
        finalize_event="finalize",
    )
    machine.add_transition(
        trigger="start_monitoring",
        source="pending",
        dest="idle",
    )
    # Restoring a saved 'blocked' or a running timer with a light on: straight
    # from pending, since the way through idle runs on_enter_idle, which
    # switches the lights off. restore_active_timer(resume=<seconds to run>);
    # a restore that settles in idle uses start_monitoring(quiet=True).
    machine.add_transition(trigger="restore_blocked", source="pending", dest="blocked")
    machine.add_transition(trigger="restore_active_timer", source="pending", dest="active")

    machine.add_transition(trigger="constrain", source="*", dest="constrained")
    machine.add_transition(
        trigger="override",
        source=["pending", "idle", "active_timer", "blocked"],
        dest="overridden",
    )

    machine.add_transition(
        trigger="activate",
        source=["idle", "blocked"],
        dest="active",
    )
    machine.add_transition(
        trigger="activate", source="active_timer", dest=None, after="_reset_timer"
    )

    # Idle
    # machine.add_transition(trigger='sensor_off',           source='idle',              dest=None)
    # Lux constraint: gate ONLY this transition — the one that turns lights on
    # from a fully off room. The other sensor_on paths (active_timer resets,
    # idle → blocked, blocked re-entry) deal with lights that are already on,
    # where the reading is inflated by the controlled lights themselves.
    machine.add_transition(
        trigger="sensor_on",
        source="idle",
        dest="active",
        conditions=["is_state_entities_off", "is_lux_constraint_satisfied"],
    )
    machine.add_transition(
        trigger="sensor_on",
        source="idle",
        dest="active",
        conditions=["is_state_entities_on"],
        unless="is_block_enabled"
    )
    machine.add_transition(
        trigger="sensor_on",
        source="idle",
        dest="blocked",
        conditions=["is_state_entities_on", "is_block_enabled"],
    )
    machine.add_transition(trigger="enable", source="idle", dest=None, conditions=["is_state_entities_off"])

    # Blocked
    machine.add_transition(trigger="enable", source="blocked", dest="idle", conditions=["is_state_entities_off"])
    machine.add_transition(
        trigger="sensor_on", source="blocked", dest="blocked", conditions=["is_block_enabled"]
    )  # re-entering self-transition (on_enter callback executed.)

    # Overridden
    # machine.add_transition(trigger='enable', source='overridden', dest='idle')
    machine.add_transition(
        trigger="enable",
        source="overridden",
        dest="idle",
        conditions=["is_state_entities_off"],
    )
    # If a device leaving overridden is on, we do not necessarily want to shut it off immediately. We simply want EC to stop controlling it. To do that, we'll move it to active, to simulate an EC trigger, and we'll see if it exits on its own. This works for event sensors, of course, and it will also work for duration sensors if the current state is on. A duration sensor that is off now will never expire on its own, though, so in that case, we'll assume the target is 'idle'.
    machine.add_transition(
        trigger="enable",
        source="overridden",
        dest="active",
        conditions=["is_state_entities_on", "is_event_sensor"],
    )
    machine.add_transition(
        trigger="enable",
        source="overridden",
        dest="active",
        conditions=["is_state_entities_on", "is_sensor_on"],
    )  # This could be duration && on, but it will also work for any event sensor, so it's simpler to just write 'on'
    machine.add_transition(
        trigger="enable",
        source="overridden",
        dest="idle",
        conditions=["is_state_entities_on", "is_duration_sensor", "is_sensor_off"],
    )

    machine.add_transition(
        trigger="enter", source="active", dest="active_timer", unless="will_stay_on"
    )
    machine.add_transition(
        trigger="enter",
        source="active",
        dest="active_stay_on",
        conditions="will_stay_on",
    )

    # Active Timer
    machine.add_transition(
        trigger="sensor_on", source="active_timer", dest=None, after="_reset_timer"
    )
    # machine.add_transition(trigger='sensor_off',           source='active_timer',      dest=None,              conditions=['is_event_sensor'])
    machine.add_transition(
        trigger="sensor_off_duration",
        source="active_timer",
        dest="idle",
        conditions=["is_timer_expired"],
    )
    # The following two transitions must be kept seperate because they have
    # special conditional logic that cannot be combined.
    machine.add_transition(
        trigger="timer_expires",
        source="active_timer",
        dest="idle",
        conditions=["is_event_sensor"],
    )
    machine.add_transition(
        trigger="timer_expires",
        source="active_timer",
        dest="idle",
        conditions=["is_duration_sensor", "is_sensor_off"],
    )
    # machine.add_transition(trigger='block_timer_expires', source='blocked', dest='idle')
    machine.add_transition(
        trigger="block_timer_expires",
        source="blocked",
        dest="active",
        conditions=["is_state_entities_on", "is_event_sensor"],
    )
    machine.add_transition(
        trigger="block_timer_expires",
        source="blocked",
        dest="active",
        conditions=["is_state_entities_on", "is_sensor_on"],
    )  # This could be duration && on, but it will also work for any event sensor, so it's simpler to just write 'on'
    machine.add_transition(
        trigger="block_timer_expires",
        source="blocked",
        dest="idle",
        conditions=["is_state_entities_on", "is_duration_sensor", "is_sensor_off"],
    )

    # Active Timer
    machine.add_transition(
        trigger="control",
        source="active_timer",
        dest="idle",
        conditions=["is_state_entities_off"]
    )
    machine.add_transition(trigger="control", source="active_timer",
                           dest="blocked", conditions=["is_state_entities_on", "is_block_enabled"])
    # When block is disabled, "control" will reset the active timer
    machine.add_transition(trigger="control", source="active_timer",
                           dest=None, after="_reset_timer", conditions=["is_state_entities_on"], unless="is_block_enabled")
    # Manually enable blocked state
    machine.add_transition(trigger="block_enable", source="active_timer",
                           dest="blocked", conditions=["is_state_entities_on", "is_block_enabled"])

    # machine.add_transition(trigger='sensor_off',           source='active_stay_on',    dest=None)
    # machine.add_transition(trigger="timer_expires", source="active_stay_on", dest=None)
    machine.add_transition(
        trigger="enable",
        source="active_stay_on",
        dest="idle",
        conditions=["is_state_entities_off"]
    )
    # Constrained
    machine.add_transition(
        trigger="enable",
        source="constrained",
        dest="idle",
        conditions=["is_override_state_off"],
    )
    machine.add_transition(
        trigger="enable",
        source="constrained",
        dest="overridden",
        conditions=["is_override_state_on"],
    )
    # Enter blocked state when component is enabled and entity is on
    machine.add_transition(trigger="blocked", source="constrained", dest="blocked", conditions=["is_block_enabled"])

    # Phase 4 fix: blocked must also be reachable from idle.
    # start_time_callback fires while the machine is already idle (e.g. after a
    # HA restart inside the active window, or an override toggled off before
    # start_time) and calls blocked() when state entities are on.  Without this
    # transition that raises MachineError, and HA keeps re-firing the failed
    # point-in-time callback, producing an error storm (observed 2026-07-08).
    # Mirrors the existing sensor_on: idle → blocked rule.
    machine.add_transition(trigger="blocked", source="idle", dest="blocked", conditions=["is_block_enabled"])

    # Phase 1 fix: block_timer_expires → idle when state entities are already off.
    # Without this transition the SM could be stuck in blocked when entities turned
    # off before the block timeout fired.
    machine.add_transition(
        trigger="block_timer_expires",
        source="blocked",
        dest="idle",
        conditions=["is_state_entities_off"],
    )

    # Phase 2 fix: catch-all for block_timer_expires → idle.
    # Covers the case where state entities are still reported as on (e.g. due to
    # slow cloud/gateway feedback from integrations like Overkiz/Tahoma) but the
    # trigger sensor has already turned off.  Without this transition the controller
    # would remain stuck in blocked indefinitely because none of the earlier
    # block_timer_expires conditions (which require is_sensor_on or
    # is_state_entities_off) would match.  This transition is unconditional so it
    # acts as a guaranteed fallback for every case not handled above.
    machine.add_transition(
        trigger="block_timer_expires",
        source="blocked",
        dest="idle",
    )

    # Phase 3: force_activate — triggered by forced sensors.
    # Bypasses blocked, constrained and overridden states entirely, and ends
    # the wait of a restored timer in pending (9.11.4).
    machine.add_transition(
        trigger="force_activate",
        source=["pending", "idle", "blocked", "constrained", "overridden", "active_timer"],
        dest="active",
    )

    for myconfig in config[DOMAIN]:
        _LOGGER.info("Domain Configuration: " + str(myconfig))
        for key, config in myconfig.items():
            if not config:
                config = {}

            # _LOGGER.info("Config Item %s: %s", str(key), str(config))
            config["name"] = key
            m = None
            m = EntityController(hass, config, machine)
            # machine.add_model(m.model)
            # m.model.after_model(config)
            devices.append(m)

    await component.async_add_entities(devices)

    _LOGGER.info("The %s component is ready!", DOMAIN)

    return True


class EntityController(entity.Entity):
    from .entity_services import (
        async_entity_service_activate as async_activate,
        async_entity_service_clear_block as async_clear_block,
        async_entity_service_enable_block as async_enable_block,
        async_entity_service_enable_stay_mode as async_enable_stay_mode,
        async_entity_service_disable_stay_mode as async_disable_stay_mode,
        async_entity_service_set_night_mode as async_set_night_mode,
    )

    def __init__(self, hass, config, machine):
        self.attributes = {}
        self.may_update = False
        self.model = None
        self.context_id = None
        self.friendly_name = config.get(CONF_NAME, "Motion Light")
        if "friendly_name" in config:
            self.friendly_name = config.get("friendly_name")
        try:
            self.model = Model(hass, config, machine, self)
        except AttributeError as e:
            _LOGGER.error(
                "Configuration error! Please ensure you use plural keys for lists. e.g. sensors, entities." + e
            )
        event.async_call_later(hass, 1, self.do_update)

    @property
    def state(self):
        """Return the state of the entity."""
        return self.model.state

    @property
    def name(self):
        """Return the state of the entity."""
        return self.friendly_name

    @property
    def icon(self):
        """Return the entity icon."""
        if self.model.state == "idle":
            return "mdi:circle-outline"
        if self.model.state == "active":
            return "mdi:check-circle"
        if self.model.state == "active_timer":
            return "mdi:timer-outline"
        if self.model.state == "constrained":
            return "mdi:cancel"
        if self.model.state == "overridden":
            return "mdi:timer-off-outline"
        if self.model.state == "blocked":
            return "mdi:close-circle"
        return "mdi:eye"

    @property
    def state_attributes(self):
        """Return the state of the entity."""
        return self.attributes.copy()

    def reset_state(self):
        """ Reset state attributes by removing any state specific attributes when returning to idle state """
        _LOGGER.debug("Resetting state")
        att = {}

        PERSISTED_STATE_ATTRIBUTES = [
            "last_triggered_by",
            "last_triggered_at",
            CONF_STATE_ENTITIES,
            "control_entities",
            "sensor_entities",
            "override_entities",
            CONF_DELAY,
            "sensor_type",
            "mode",
            "start_time",
            "end_time",
            "lux_entity",
            "lux_threshold",
            "lux_bright_states",
            "block_on_external_on",
        ]
        for k, v in self.attributes.items():
            if k in PERSISTED_STATE_ATTRIBUTES:
                att[k] = v

        self.attributes = att
        self.do_update()

    @callback
    def do_update(self, wait=False, **kwargs):
        """ Schedules an entity state update with HASS """
        # _LOGGER.debug("Scheduled update with HASS")
        if self.may_update:
            self.schedule_update_ha_state(True)

    def set_attr(self, k, v):
        if k == CONF_DELAY:
            v = str(v) + "s"
        self.attributes[k] = v

    # HA Callbacks
    async def async_added_to_hass(self):
        """Register update dispatcher."""
        self.may_update = True

    @property
    def should_poll(self) -> bool:
        """EntityController will push its state to HA"""
        return False

class _DebounceWindow:
    """External state changes waiting for confirmation (Model._debounce_state_change)."""

    __slots__ = ("state", "snapshot", "context", "handle")

    def __init__(self, state):
        self.state = state    # EC state the window was opened in
        self.snapshot = {}    # entity_id -> [first old State or None, attribute-only so far, last State seen]
        self.context = None   # context of the latest change; parent of EC's reaction
        self.handle = None    # async_call_later cancel; never called


class _RestoreWait:
    """A restored running timer waiting in pending for its lights (Model._restore_timer)."""

    __slots__ = ("expiry", "unsubs", "returned", "switched_on", "settle")

    def __init__(self, expiry):
        self.expiry = expiry     # saved run-out of the timer (naive local time)
        self.unsubs = []         # state listener remover, async_call_later cancel
        self.returned = {}       # light -> loop time it came back from unavailable/unknown
        self.switched_on = set() # lights switched on during the wait, not by a comeback
        self.settle = None       # cancel of the re-check once the lights came back off

class Model:
    """ Represents the transitions state machine model """

    def __init__(self, hass, config, machine, entity):
        self.ec_startup_time = datetime.now()

        self.hass = hass  # backwards reference to hass object
        self.entity = entity  # backwards reference to entity containing this model

        self.config = (
            {}
        )  # new way of storing configuration (avoids having an attribue for each)
        self.config = config
        self.debug_day_length = config.get("day_length", None)
        self.stateEntities = []
        self.controlEntities = []
        self.sensorEntities = []
        self.forcedSensorEntities = []
        self.eventSensorTypes = []
        self._event_sensor_cancel_callbacks = []
        self.triggerOnDeactivate = []
        self.triggerOnActivate = []
        self.timer_handle = None
        self._expiry_time = None   # when the running timer runs out (saved for a restart)
        self._restore_wait = None  # _RestoreWait while a restored timer waits for its lights
        self.block_timer_handle = None
        self.sensor_type = None
        self.night_mode = None
        self.state_attributes_ignore = []
        self.backoff = False
        self.backoff_count = 0
        self.light_params_day = {}
        self.light_params_night = {}
        self.lightParams = {}
        self.name = None
        self.stay = False
        self.start = None
        self.end = None
        self.reset_count = None
        self.transition_behaviours = {}
        # logging.setFormatter(logging.Formatter(FORMAT))
        self.log = logging.getLogger(__name__ + "." + config.get(CONF_NAME))
        self.ignored_event_sources = []
        self.context = None
        self._store = None  # HA storage for state persistence
        self.luxEntity = None
        self.lux_threshold = None
        self.lux_bright_states = []
        self.lux_recheck_delay = 1.0
        self._lux_recheck_handle = None
        self._lux_recheck_entity = None
        self._lux_blocked = False
        self.state_change_debounce = DEFAULT_STATE_CHANGE_DEBOUNCE
        self._debounce_window = None
        self.block_on_external_on = True
        # set while a trigger leaves blocked (cleared in finalize); loop times
        # of the block start / last regular-sensor entry and of the block timer
        self._block_ending = False
        self._block_anchor = self._block_timer_started = 0.0
        self.night_mode_entity = None
        self.night_mode_entity_states = []
        self.nightControlEntities = []
        # the target set chosen at the most recent activation; turn-off must
        # use the same set even if day/night flipped while the timer ran
        self.activeControlEntities = []
        self._state_entities_explicit = False

        self.log.debug(
            "Initialising EntityController entity with this configuration: "
        )
        self.log.debug(
            pprint.pformat(config)
        )
        self.name = config.get(CONF_NAME, "Unnamed Entity Controller")
        self.ignored_event_sources = []

        machine.add_model(
            self
        )  # add here because machine generated methods are being used in methods below.

        event.async_call_later(self.hass, STARTUP_DELAY, self.startup_delay_callback)

    async def startup_delay_callback(self, evt):
        config = self.config
        self.config_static_strings(config)
        self.config_control_entities(config)
        self.config_state_entities(
            config
        )  # must come after config_control_entities (uses control entities if not set)
        self.config_sensor_entities(config)
        self.config_hold_sensor_entities(config)
        self.config_forced_sensor_entities(config)
        self.config_event_sensors(config)
        self.config_override_entities(config)
        self.config_lux_constraint(config)
        self.config_transition_behaviours(config)
        self.config_off_entities(config)
        self.config_on_entities(config)
        self.config_normal_mode(config)
        self.config_night_mode(
            config
        )  # must come after normal_mode (uses normal mode parameters if not set)
        self.config_state_attributes_ignore(config)
        self.config_times(config)
        self.config_other(config)
        self.prepare_service_data()

        # Phase 2: Set up state persistence store and register shutdown handler
        self._store = Store(self.hass, STORAGE_VERSION, self._storage_key())
        self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, self._async_save_state)

        # Phase 2: Attempt to restore persisted state before the first transition
        restored = await self._async_restore_state()

        if not restored:
            if len(self.overrideEntities) > 0 and self.is_override_state_on():
                self.override()
                self.update(overridden_at=str(datetime.now()))
            else:
                self.start_monitoring()

    def update(self, wait=False, **kwargs):
        """ Called from different methods to report a state attribute change """
        # self.log.debug("Update called with {}".format(str(kwargs)))
        for k, v in kwargs.items():
            if v is not None:
                self.entity.set_attr(k, v)

        if wait == False:
            self.entity.do_update()

    def finalize(self, *_args, **_kwargs):
        # transitions passes a trigger's arguments to every callback
        # (restore_active_timer(resume=...), start_monitoring(quiet=...)).
        self._block_ending = False
        self.entity.do_update()

    # =====================================================
    # S T A T E   C H A N G E   C A L L B A C K S
    # =====================================================

    @callback
    def sensor_state_change(self, event):
        """ State change callback for sensor entities """
        entity = event.data["entity_id"]
        old = event.data["old_state"]
        new = event.data["new_state"]
        if new is None:
            # The entity is gone (platform reload / removed from the system),
            # so there is nothing to evaluate. Without this guard the log call
            # below already raises "'NoneType' object has no attribute ..."
            # (HA 2026.7 sends state_changed with new_state=None on removal).
            self.log.debug(
                "%s :: new_state is None (entity %s removed) — ignoring",
                "sensor_state_change", str(entity))
            return
        self.log.debug("sensor_state_change :: %10s Sensor state change to: %s" % ( pprint.pformat(entity), new.state))
        self.log.debug("sensor_state_change :: state: " +  pprint.pformat(self.state))

        try:
            if new.state == old.state:
                self.log.debug("sensor_state_change :: Ignore attribute only change")
                return
        except AttributeError:
            self.log.debug("sensor_state_change :: old NoneType")
            pass

        if self.matches(new.state, self.SENSOR_ON_STATE) and (
            self.is_idle() or self.is_active_timer() or self.is_blocked()
        ):
            self.set_context(new.context)
            self.update(last_triggered_by=entity)
            self.sensor_on()
            if self.is_idle() and getattr(self, "_lux_blocked", False):
                self._schedule_lux_recheck(entity)

        if (
            self.matches(new.state, self.SENSOR_OFF_STATE)
            and self.is_duration_sensor()
            and self.is_active_timer()
        ):
            self.set_context(new.context)
            self.update(last_triggered_by=entity, sensor_turned_off_at=datetime.now())

            # If configured, reset timer when duration sensor goes off
            if self.config[CONF_SENSOR_RESETS_TIMER]:
                self.log.debug("sensor_state_change :: CONF_SENSOR_RESETS_TIMER")
                self.update(
                    notes="The sensor turned off and reset the timeout. Timer started."
                )
                self._reset_timer()
            else:
                # We only care about sensor off state changes when the sensor is a duration sensor and we are in active_timer state.
                self.sensor_off_duration()
                self.log.debug("sensor_state_change :: CONF_SENSOR_RESETS_TIMER - normal")

    def _schedule_lux_recheck(self, entity):
        """Give a lux-blocked activation one second look at the reading.

        Integrations that publish occupancy and illuminance from a single
        device message (Zigbee2MQTT + Aqara PIR) deliver them as two HA state
        changes whose order is not stable across HA restarts. When the
        occupancy change lands first, the constraint compares against the
        *previous* illuminance report - typically taken while the lights were
        still on - and blocks a legitimate activation in a dark room
        (2026-09-04: ec_204 read 62 lx while the fresh report, 2 ms later,
        said 6 lx; ec_206 the same with 37 lx). One re-check after
        ``lux_recheck_delay`` seconds, with the sensor still on, picks up the
        late reading. A room that really is bright stays blocked;
        ``lux_recheck_delay: null`` disables the retry.
        """
        delay = getattr(self, "lux_recheck_delay", None)
        if not delay:
            return
        if getattr(self, "_lux_recheck_handle", None) is not None:
            return
        self.log.debug(
            "_schedule_lux_recheck :: activation by %s blocked on a possibly "
            "stale lux reading; re-checking in %.1f s",
            entity, delay,
        )
        self._lux_recheck_entity = entity
        self._lux_recheck_handle = event.async_call_later(
            self.hass, delay, self._lux_recheck
        )

    @callback
    def _lux_recheck(self, _now=None):
        """Retry the idle -> active transition once the lux entity caught up."""
        self._lux_recheck_handle = None
        if not self.is_idle():
            return
        triggered_by = getattr(self, "_lux_recheck_entity", None)
        for e in self.sensorEntities:
            s = self.hass.states.get(e)
            if s is not None and self.matches(s.state, self.SENSOR_ON_STATE):
                break
        else:
            self.log.debug(
                "_lux_recheck :: no sensor is on any more; nothing to retry")
            return
        self.log.debug(
            "_lux_recheck :: retrying activation, sensor %s is still on", e)
        self.update(
            last_triggered_by=triggered_by or e,
            notes="Lux re-check after a possibly stale reading",
        )
        self.sensor_on()

    @callback
    def hold_sensor_state_change(self, event):
        """Hold sensor: keeps the light on, but never switches it on."""
        entity = event.data["entity_id"]
        old = event.data["old_state"]
        new = event.data["new_state"]
        if new is None:
            return
        try:
            if old is not None and new.state == old.state:
                return  # attribute-only change
        except AttributeError:
            pass

        if self.matches(new.state, self.SENSOR_ON_STATE):
            # DELIBERATELY not calling sensor_on() — a hold sensor must never
            # switch the light on.
            # With the timer already running, act as a keep-alive; in blocked,
            # restart the block timer in place (_hold_rearm_block).
            if self.is_active_timer():
                self.set_context(new.context)
                self.update(last_triggered_by=f"hold:{entity}",
                            notes="Hold sensor is on, keeping the light on")
                self._cancel_timer()
                self.update(expires_at="pending sensor")
            elif self.is_blocked():
                self._hold_rearm_block(entity)
            else:
                self.log.debug(
                    "hold_sensor_state_change :: %s is ON, but not switching anything on (state %s)",
                    entity, self.state)
            return

        if (
            self.matches(new.state, self.SENSOR_OFF_STATE)
            and self.is_duration_sensor()
            and self.is_active_timer()
        ):
            # Hold sensor went off — the timer only starts once nothing holds
            # it any more.
            if self.is_sensor_on():
                self.log.debug(
                    "hold_sensor_state_change :: %s is OFF, but another sensor still holds", entity)
                return
            self.set_context(new.context)
            self.update(last_triggered_by=f"hold:{entity}", sensor_turned_off_at=datetime.now(),
                        notes="Hold sensor turned off, timer started")
            self._reset_timer()

    def _hold_rearm_block(self, entity):
        """A hold sensor keeps a block alive (2026-11-10): the block timer
        restarts in place, at most once per HOLD_REARM_THROTTLE seconds and
        within hold_max_seconds of the block start or the last regular-sensor
        edge; never a re-entry of blocked."""
        if not self.block_timeout:
            return
        now, cap = self.hass.loop.time(), getattr(self, "hold_max_seconds", None)
        if cap and now - self._block_anchor >= cap:
            self.log.debug(
                "hold_sensor_state_change :: %s is on, but no regular sensor for %s s; "
                "the block runs out", entity, cap)
            return
        if now - self._block_timer_started < HOLD_REARM_THROTTLE:
            return
        self._start_block_timer()
        self.update(notes="Hold sensor %s is on, block timer restarted" % entity)

    @callback
    def override_state_change(self, event):
        """ State change callback for override entities """
        entity = event.data["entity_id"]
        old = event.data["old_state"]
        new = event.data["new_state"]
        if new is None:
            # The entity is gone (platform reload / removed from the system),
            # so there is nothing to evaluate. Without this guard the log call
            # below already raises "'NoneType' object has no attribute ..."
            # (HA 2026.7 sends state_changed with new_state=None on removal).
            self.log.debug(
                "%s :: new_state is None (entity %s removed) — ignoring",
                "override_state_change", str(entity))
            return
        self.log.debug("override_state_change :: Override state change entity=%s, old=%s, new=%s" % ( entity, old, new))
        if self.matches(new.state, self.OVERRIDE_ON_STATE) and (
            self.is_active()
            or self.is_active_timer()
            or self.is_idle()
            or self.is_blocked()
        ):
            self.set_context(new.context)
            self.update(overridden_by=entity)
            self.override()
            self.update(overridden_at=str(datetime.now()))
        if (
            self.matches(new.state, self.OVERRIDE_OFF_STATE)
            and self.is_override_state_off()
            and self.is_overridden()
        ):
            self.set_context(new.context)
            self.enable()

    @callback
    def state_entity_state_change(self, event):
        entity = event.data["entity_id"]
        old = event.data["old_state"]
        new = event.data["new_state"]
        if new is None:
            # The entity is gone (platform reload / removed from the system),
            # so there is nothing to evaluate. Without this guard the log call
            # below already raises "'NoneType' object has no attribute ..."
            # (HA 2026.7 sends state_changed with new_state=None on removal).
            self.log.debug(
                "%s :: new_state is None (entity %s removed) — ignoring",
                "state_entity_state_change", str(entity))
            return
        """ State change callback for state entities. This can be called with either a state change or an attribute change. """
        self.log.debug(
            "state_entity_state_change :: [ Entity: %s, Context: %s ]\n\tOld state: %s\n\tNew State: %s",
            str(entity),
            str(new.context),
            str(old),
            str(new)
        )
        # --- Link outage (2026-09-19) --------------------------------------
        # `unavailable` is not a state of the light, it is a lost link
        # (localtuya blip; light.101_a dropped 10x in 14 days). Treating it as
        # "switched off externally" made EC leave active_timer for idle and
        # fire a turn_off the offline device never received — and that doomed
        # call armed the grace period, which then swallowed the relay's return
        # 87 ms later (2026-09-18 19:14:56: 101_a came back on, EC stayed
        # idle, the light burned 39 min until block_timeout). So: ignore the
        # drop, keep the timer running, and let the resync below deal with
        # whatever the device reports when it returns.
        if getattr(new, "state", None) == STATE_UNAVAILABLE:
            self.log.info(
                "state_entity_state_change :: %s became unavailable (EC state: %s) — ignoring, waiting for it to return",
                str(entity), self.state,
            )
            return

        # --- Resync after unavailability (2026-08-23, moved ahead of the
        # grace-period check 2026-09-19: a comeback is never our own echo) --
        # A controlled entity (relay/light) dropped off WiFi and came back.
        # That is NOT manual control, so it must not be evaluated as external
        # control (it used to push EC into 'blocked', after which the timer
        # never completed again).
        # It is also the only moment when the state can be genuinely stuck:
        # if EC wanted to turn the light off during the outage, that command
        # was dropped.
        #
        # Replaces the naive Turnoff_light_from_unavailable automation, which
        # turned lights off UNCONDITIONALLY. Measured over 7 days: 11 firings,
        # 10 of them wrong (91 %) — in 9 cases a sensor in the room was active,
        # and once EC turned the light back on after just 2 s (visible flash).
        # Hence: only turn off when EC is 'idle' AND no sensor reports motion.
        try:
            came_back = (
                old is not None
                and getattr(old, "state", None) == STATE_UNAVAILABLE
                and getattr(new, "state", None) != STATE_UNAVAILABLE
            )
        except AttributeError:
            came_back = False
        if came_back:
            # A pending debounce judges this entity by what it reports now.
            window = self._debounce_window
            if window is not None and entity in window.snapshot:
                window.snapshot[entity][2] = new
            is_on = self.matches(new.state, self.CONTROL_ON_STATE)
            self.log.info(
                "state_entity_state_change :: %s came back from unavailable as '%s' (EC state: %s) — resync",
                str(entity), str(new.state), self.state,
            )
            if self.is_active_timer() or self.is_active_stay_on():
                # EC wants the light on: if it came back off, turn it on.
                if not is_on:
                    self.log.info("resync :: timer is running but entity came back off — turning on")
                    self.turn_on_control_entities()
                return
            if is_on and self.is_idle():
                if self.is_sensor_on():
                    # Someone is in the room and the light is on. Leaving EC in
                    # 'idle' is NOT enough: the next sensor edge would call
                    # sensor_on() on an already-on entity, which deliberately
                    # lands in 'blocked' (block_timeout refreshed by every
                    # further edge), so EC would never manage the light again
                    # and a human has to turn it off.
                    # Measured 2026-08-23: ec_105_e went blocked at 20:44 after
                    # the relay came back on, was re-blocked by every kitchen PIR
                    # edge, and Jan had to switch 1NP off by hand at 21:34.
                    # force_activate() adopts the light instead: EC enters
                    # 'active', starts its timer and turns the light off after
                    # the delay once the room empties.
                    self.log.info("resync :: on and a sensor is active — adopting it (force_activate)")
                    self.force_activate()
                    return
                self.log.info("resync :: on, EC is idle and sensors are quiet — clearing stuck state")
                self.turn_off_control_entities()
            return
        # -----------------------------------------------------------------
        if self.is_ignored_context(new.context):
            self.log.debug("state_entity_state_change :: Ignoring this state change because it came from %s" % (new.context.id))
            return
        if self.is_within_grace_period():
            if self.is_idle() and self._external_on_edge(old, new):
                # EC's own echo in idle is an off: judge an ON after the grace
                # period; capped so a clock step back cannot stretch it
                left = min(max((self.ignore_state_changes_until - datetime.now()).total_seconds(), 0),
                           self.grace_period)
                self._debounce_state_change(entity, old, new, False,
                                            delay=left + max(self.state_change_debounce or 0, 1.0))
                return
            self.log.debug("state_entity_state_change :: Ignoring this state change because we are within the grace period (until %s)", self.ignore_state_changes_until)
            return


        #  If the state changed, we definitely want to handle the transition. If only attributes changed, we'll check if the new attributes are significant (i.e., not being ignored).
        attr_only = False
        try:
            attr_only = old is not None and old.state == new.state
            if attr_only:
                if self._significant_attributes(old) == self._significant_attributes(new):
                    self.log.debug("state_entity_state_change :: insignificant attribute change - Ignore the state change altogether")
                    return
                self.log.debug("state_entity_state_change :: A significant attribute changed and will be handled")
        except AttributeError as a:
            attr_only = False
            self.log.debug("state_entity_state_change :: cannot compare the two states, handling the change: %s", a)
        if self._debounce_state_change(entity, old, new, attr_only):
            return
        self.set_context(new.context)
        if self.is_idle():
            # acts only with the debounce off; with it on, an ON edge went into the window
            if self._external_on_edge(old, new):
                self._block_external_on([entity])
            return
        if self.is_active_timer():
            self.log.debug("state_entity_state_change :: We are in active timer and the state of observed state entities changed.")
            self.control()

        if self.is_blocked() or self.is_active_stay_on(): # if statement required to avoid MachineErrors, cleaner than adding transitions to all possible states.
            self.enable()

    def _significant_attributes(self, state):
        return {
            key: value for key, value in state.attributes.items()
            if key not in self.state_attributes_ignore
        }

    def _debounce_state_change(self, entity, old, new, attr_only, delay=None):
        """Hold an external change for state_change_debounce seconds.

        Returns False when the change is handled at once (debounce off, or a
        state without a reaction). The deadline runs from the first change and
        is not extended; _state_debounce_fire acts once if anything persisted.
        In idle only an ON edge opens a window. delay: an ON edge in idle
        inside the grace period, judged after it.
        """
        if delay is None:
            if not self.state_change_debounce or self.state not in (
                "active_timer", "blocked", "active_stay_on", "idle"
            ):
                return False
            delay = self.state_change_debounce
        window = self._debounce_window
        if window is None or window.state != self.state:
            if self.state == "idle" and not self._external_on_edge(old, new):
                return False
            window = _DebounceWindow(self.state)
            self._debounce_window = window
            window.handle = event.async_call_later(
                self.hass, delay,
                functools.partial(self._state_debounce_fire, window),
            )
            self.log.debug(
                "state_entity_state_change :: %s changed in %s; acting only if it persists for %.1f s",
                entity, self.state, delay)
        else:
            self.log.debug("state_entity_state_change :: %s changed; check already pending", entity)
        entry = window.snapshot.setdefault(entity, [old, attr_only, new])
        entry[1] = entry[1] and attr_only
        entry[2] = new
        window.context = new.context
        return True

    @callback
    def _state_debounce_fire(self, window, _now=None):
        """Act once on the changes in window if any of them persisted."""
        if self._debounce_window is not window:
            return
        self._debounce_window = None
        if self.state != window.state:
            self.log.debug("_state_debounce_fire :: EC left %s, dropping the change of %s", window.state, ", ".join(window.snapshot))
            return
        if self.is_within_grace_period():
            self.log.info("_state_debounce_fire :: dropping the change of %s: EC called a service meanwhile (grace period)", ", ".join(window.snapshot))
            return
        result = {e: self._debounce_persisted(e, *entry) for e, entry in window.snapshot.items()}
        gone = [e for e, r in result.items() if r is None]
        back = [e for e, r in result.items() if r is not None and not r]
        changed = [e for e, r in result.items() if r]
        # in idle only a take-over logs at INFO
        log = self.log.debug if window.state == "idle" else self.log.info
        if gone:
            log("_state_debounce_fire :: %s is gone - not compared", ", ".join(gone))
        if not changed:
            if back:
                log("_state_debounce_fire :: %s is back to its previous state - ignoring the bounce (EC state: %s)", ", ".join(back), self.state)
            return
        if window.state != "idle":
            self.log.info("_state_debounce_fire :: %s changed for good - handling it (EC state: %s)", ", ".join(changed), self.state)
        self.set_context(window.context)
        if self.state != window.state:
            self.log.debug("_state_debounce_fire :: EC left %s meanwhile, not acting", window.state)
        elif window.state == "active_timer":
            self.control()
        elif window.state == "idle":
            self._block_external_on([e for e in changed if self._external_on_edge(
                window.snapshot[e][0], self._debounce_current(e, window.snapshot[e][2]))])
        else:
            self.enable()

    def _debounce_persisted(self, entity, old, attr_only, last):
        """True if entity still differs from its state before the window,
        False if it is back, None if it is gone (not compared).

        An unavailable entity is judged by the last state it reported in the
        window: a real off followed by a link drop still counts.
        """
        cur = self._debounce_current(entity, last)
        if cur is None:
            return None
        old_state = getattr(old, "state", None)
        if old_state is None or cur.state != old_state:
            return True
        return attr_only and self._significant_attributes(cur) != self._significant_attributes(old)

    def _debounce_current(self, entity, last):
        """The entity's state now; while unavailable, the last state it reported."""
        cur = self.hass.states.get(entity)
        if cur is not None and cur.state == STATE_UNAVAILABLE:
            cur = last
        return cur

    def _external_on_blocks(self):
        """A light switched on outside EC while idle puts EC in blocked."""
        return self.block_on_external_on is True and self.is_block_enabled()

    def _external_on_edge(self, old, new):
        """An ON edge that counts for _external_on_blocks: off, unknown or
        nothing before, on now. A return from unavailable is the resync's."""
        if not self._external_on_blocks():
            return False
        new_state, old_state = getattr(new, "state", None), getattr(old, "state", None)
        if new_state is None or not self.matches(new_state, self.STATE_ON_STATE):
            return False
        return old_state is None or old_state == STATE_UNKNOWN or self.matches(old_state, self.STATE_OFF_STATE)

    def _is_control_on(self, entity):
        s = self.hass.states.get(entity)
        return s is not None and self.matches(s.state, self.CONTROL_ON_STATE)

    def _block_external_on(self, lit):
        """idle -> blocked for lights switched on outside EC; no service call."""
        if not lit or not self.is_state_entities_on():
            self.log.debug("_block_external_on :: nothing switched on for good; staying idle")
            return
        self.log.info("%s switched on outside EC while idle; blocked (block_timeout %s)",
                      ", ".join(lit), self.block_timeout)
        self.blocked()
        self.update(last_triggered_by=lit[0], notes="Switched on outside EC while idle")

    @callback
    def forced_sensor_state_change(self, ev):
        """State change callback for forced sensor entities (Phase 3).

        Forced sensors bypass blocked, constrained, and overridden states and
        immediately activate the controller.
        """
        entity_id = ev.data["entity_id"]
        old = ev.data["old_state"]
        new = ev.data["new_state"]
        self.log.debug(
            "forced_sensor_state_change :: %s → %s (state=%s)",
            entity_id,
            new.state if new else None,
            self.state,
        )

        try:
            if new.state == old.state:
                self.log.debug("forced_sensor_state_change :: Ignore attribute-only change")
                return
        except AttributeError:
            pass

        if new and self.matches(new.state, self.SENSOR_ON_STATE):
            self.set_context(new.context)
            self.update(last_triggered_by=entity_id)
            self.log.debug(
                "forced_sensor_state_change :: Force-activating from state '%s'", self.state
            )
            self.force_activate()

    @callback
    def ha_event_sensor_callback(self, event_type, ev):
        """HA bus event callback for event-bus sensors (Phase 6).

        Subscribes to arbitrary HA bus events and triggers sensor_on when
        the configured event fires, regardless of any attached event data.
        """
        self.log.debug(
            "ha_event_sensor_callback :: Received HA bus event '%s' (state=%s)",
            event_type,
            self.state,
        )
        self.set_context(None)
        self.update(last_triggered_by=f"ha_event:{event_type}")
        if self.is_idle() or self.is_active_timer() or self.is_blocked():
            self.sensor_on()

    def _start_timer(self, delay=None):
        """delay: run this many seconds instead of the configured delay
        (a timer resumed after a restart)."""
        self.log.info("_start_timer :: Light params: " + str(self.lightParams))
        if self.backoff_count == 0:
            self.previous_delay = self.lightParams.get(CONF_DELAY, DEFAULT_DELAY)
        else:
            self.log.debug(
                "_start_timer :: Backoff: %s,  count: %s, delay%s, factor: %s",
                self.backoff,
                self.backoff_count,
                self.lightParams.get(CONF_DELAY, DEFAULT_DELAY),
                self.backoff_factor,
            )
            self.previous_delay = round(self.previous_delay * self.backoff_factor, 2)
            if self.previous_delay > self.backoff_max:
                self.log.debug("Max backoff reached. Will not increase further.")
                self.previous_delay = self.backoff_max
            self.update(delay=self.previous_delay)
        if delay is None:
            delay = self.previous_delay

        expiry_time = datetime.now() + timedelta(seconds=delay)

        # not able to use async_call_later because no known way to check whether timer is active.
        # self.timer_handle = event.async_call_later(self.hass, self.previous_delay, self.timer_expire)
        self.timer_handle = Timer(delay, self.timer_expire)
        self.timer_handle.start()
        self._expiry_time = expiry_time
        self.update(expires_at=expiry_time)

    def _cancel_timer(self):
        if self.timer_handle.is_alive():
            self.timer_handle.cancel()

    def _reset_timer(self):
        self.log.debug("_reset_timer :: Resetting timer: " + str(self.backoff))
        self._cancel_timer()
        self.update(reset_at=datetime.now())
        if self.backoff:
            self.backoff_count += 1
            self.update(backoff_count=self.backoff_count)
        self._start_timer()

        return True

    def timer_expire(self):
        self.log.debug("timer_expire :: Timer expired")
        if self.is_duration_sensor() and self.is_sensor_on():  # Ignore timer expiry because duration sensor overwrites timer
            self.update(expires_at="pending sensor")
        else:
            self.log.debug("timer_expire :: Trigger timer_expires event")
            self.timer_expires()

    def block_timer_expire(self):
        # A timer replaced by a restart, or one firing outside blocked, is
        # stale (block_timer_expires from idle raises MachineError).
        t = current_thread()
        if (getattr(t, "function", None) == self.block_timer_expire and t is not self.block_timer_handle) \
                or not self.is_blocked():
            self.log.debug("block_timer_expire :: stale block timer (EC state %s), ignoring", self.state)
            return
        self.log.debug("block_timer_expire :: Blocked Timer expired")
        self.block_timer_expires()

    # =====================================================
    # S T A T E   M A C H I N E   C O N D I T I O N S
    # =====================================================
    def _override_entity_state(self):
        for e in self.overrideEntities:
            s = self.hass.states.get(e)
            try:
                state = s.state
            except AttributeError as ex:
                self.log.error(
                    "Potential configuration error: Override Entity ({}) does not exist (yet). Please check for spelling and typos. {}".format(
                        e, ex
                    )
                )
                
                continue

            if self.matches(state, self.OVERRIDE_ON_STATE):
                self.log.debug("Override entities are ON. [%s]", e)
                return e
        self.log.debug("Override entities are OFF.")
        return None

    def is_override_state_off(self):
        return self._override_entity_state() is None

    def is_within_grace_period(self):
        """Determines whether the last service call EC made was within the configured grace_period.

        This is a fallback for integrations (e.g. cloud/gateway integrations like Tahoma) that
        do not propagate the original HA context to their state-change events.  For those
        integrations the Context-API check in is_ignored_context() is insufficient because the
        delayed state update arrives with a fresh, unrelated context.  Setting grace_period to a
        value that covers the integration's worst-case latency prevents EC from entering the
        blocked state due to its own delayed state feedback.
        """
        if not self.grace_period:
            return False
        return datetime.now() < self.ignore_state_changes_until

    def is_override_state_on(self):
        return self._override_entity_state() is not None

    def _hold_sensor_is_stuck(self, entity, state_obj):
        """True when a hold sensor has been reporting ON for too long.

        A wedged presence sensor would otherwise keep the timer alive forever
        (expires_at: "pending sensor") and the light would never turn off. See
        CONF_HOLD_MAX_SECONDS for the incident this guards against.
        """
        cap = getattr(self, "hold_max_seconds", None)
        if not cap:
            return False
        try:
            held_for = (
                dt_util.utcnow() - state_obj.last_changed
            ).total_seconds()
        except (AttributeError, TypeError):
            return False
        if held_for < cap:
            return False
        self.log.warning(
            "_sensor_entity_state :: hold sensor %s has been ON for %.0f s "
            "(cap %s s) — treating it as stuck and ignoring it, otherwise the "
            "timer would never start",
            entity, held_for, cap,
        )
        return True

    def _sensor_entity_state(self):
        # Regular sensors first: they both trigger and hold, so they are always
        # honoured. Hold sensors only hold, which makes it safe to ignore one
        # that is obviously wedged (a stuck sensor in `sensors:` would just
        # re-trigger, so the same treatment there would only cause flicker).
        regular = list(self.sensorEntities)
        holds = list(getattr(self, "holdSensorEntities", []))
        for e in regular + holds:
            s = self.hass.states.get(e)
            try:
                state = s.state
            except AttributeError as ex:
                self.log.error(
                    "Potential configuration error: Sensor Entity ({}) does not exist (yet). Please check for spelling and typos. {}".format(
                        e, ex
                    )
                )
                
                continue

            if self.matches(state, self.SENSOR_ON_STATE):
                if e in holds and e not in regular and self._hold_sensor_is_stuck(e, s):
                    continue
                self.log.debug("Sensor entities are ON. [%s]", e)
                return e
        self.log.debug("Sensor entities are OFF.")
        return None

    def is_sensor_off(self):
        return self._sensor_entity_state() is None

    def is_sensor_on(self):
        return self._sensor_entity_state() is not None

    def _state_entity_state(self):
        for e in self.stateEntities:
            s = self.hass.states.get(e)
            self.log.info(s)
            try:
                state = s.state
            except AttributeError as ex:
                self.log.error(
                    "Potential configuration error: State Entity ({}) does not exist (yet). Please check for spelling and typos. {}".format(
                        e, ex
                    )
                )
                
                continue

            if self.matches(state, self.STATE_ON_STATE):
                self.log.debug("State entities are ON. [%s]", e)
                return e
        self.log.debug("State entities are OFF.")
        return None

    def is_state_entities_off(self):
        return self._state_entity_state() is None

    def is_state_entities_on(self):
        return self._state_entity_state() is not None

    def is_block_enabled(self):
        return self.disable_block is False

    def is_lux_constraint_satisfied(self):
        """Whether the illuminance constraint allows turning the lights on.

        Two sensor flavours are supported on the same ``lux_entity``:

        - numeric readings (PIR-style lux sensors) are compared against
          ``lux_threshold``: activation requires reading < threshold;
        - string states (radar-style two-level sensors reporting e.g.
          ``bright``/``dim``) block activation when the state matches one of
          ``lux_bright_states`` — the calibration lives in the sensor's own
          firmware.

        Anything else — no constraint configured, unavailable/unknown state,
        a numeric reading without a configured threshold, or a string not in
        ``lux_bright_states`` — allows activation. Failing open on a broken
        sensor is deliberate: a dead battery must degrade to pre-lux
        behaviour (lights turn on), never to a permanently dark room.
        """
        self._lux_blocked = False
        if self.luxEntity is None:
            return True
        s = self.hass.states.get(self.luxEntity)
        try:
            state = s.state
        except AttributeError:
            self.log.warning(
                "is_lux_constraint_satisfied :: Lux entity %s does not exist; failing open (activation allowed).",
                self.luxEntity,
            )
            return True

        try:
            value = float(state)
        except (TypeError, ValueError):
            value = None

        if value is not None:
            if self.lux_threshold is None:
                self.log.warning(
                    "is_lux_constraint_satisfied :: %s reports numeric %s but no lux_threshold is configured; failing open.",
                    self.luxEntity,
                    value,
                )
                return True
            if value >= self.lux_threshold:
                self.log.debug(
                    "is_lux_constraint_satisfied :: Activation blocked: %s reports %s lx >= threshold %s lx.",
                    self.luxEntity,
                    value,
                    self.lux_threshold,
                )
                self._lux_blocked = True
                self.update(lux_blocked_at=str(datetime.now()), lux_at_last_block=value)
                return False
            self.log.debug(
                "is_lux_constraint_satisfied :: %s reports %s lx < threshold %s lx; activation allowed.",
                self.luxEntity,
                value,
                self.lux_threshold,
            )
            return True

        if state in self.lux_bright_states:
            self.log.debug(
                "is_lux_constraint_satisfied :: Activation blocked: %s reports '%s' (in lux_bright_states).",
                self.luxEntity,
                state,
            )
            self._lux_blocked = True
            self.update(lux_blocked_at=str(datetime.now()), lux_at_last_block=state)
            return False
        self.log.debug(
            "is_lux_constraint_satisfied :: %s reports non-bright state '%s'; activation allowed.",
            self.luxEntity,
            state,
        )
        return True

    def will_stay_on(self):
        return self.stay

    def is_night(self):
        if self.night_mode is None:
            return False  # if night mode is undefined, it's never night :)

        self.log.debug("NIGHT MODE ENABLED: " + str(self.night_mode))

        # state-entity-driven night detection (e.g. input_select.house_mode);
        # an unavailable/missing entity fails open to DAY behaviour — a broken
        # helper must never dim the house, only skip the night tweaks
        if self.night_mode_entity is not None:
            s = self.hass.states.get(self.night_mode_entity)
            state = getattr(s, "state", None)
            if state is None:
                self.log.warning(
                    "is_night :: night_mode entity %s does not exist; treating as day.",
                    self.night_mode_entity,
                )
            elif state in self.night_mode_entity_states:
                self.log.debug(
                    "is_night :: %s is '%s' (in entity_states) -> night.",
                    self.night_mode_entity,
                    state,
                )
                return True

        # time-window detection (original behaviour); may coexist with the
        # entity above — whichever says "night" wins
        if self.night_mode.get(CONF_START_TIME) and self.night_mode.get(
            CONF_END_TIME
        ):
            return self.now_is_between(
                self.night_mode[CONF_START_TIME], self.night_mode[CONF_END_TIME]
            )
        return False

    def is_event_sensor(self):
        return self.sensor_type == SENSOR_TYPE_EVENT

    def is_duration_sensor(self):
        return self.sensor_type == SENSOR_TYPE_DURATION

    def is_timer_expired(self):
        expired = self.timer_handle.is_alive() == False
        return expired

    def does_sensor_reset_timer(self):
        return self.config[CONF_SENSOR_RESETS_TIMER]

    # =====================================================
    # S T A T E   M A C H I N E   C A L L B A C K S
    # =====================================================
    def on_exit_pending(self, *_args, **_kwargs):
        # Every way out of pending (restore, override, constrain, a forced
        # sensor) ends a restored timer's wait. Takes the trigger's arguments
        # like every callback (restore_active_timer(resume=...)).
        self._restore_wait_end()

    def on_enter_idle(self, quiet=False):
        """quiet: a restore settles in idle without the on_enter_idle
        behaviour (start_monitoring(quiet=True))."""
        self.log.debug("Entering idle")
        self._debounce_window = None
        # Entering idle due to no events, set a new context with no parent
        self.set_context(None)
        if not quiet:
            self.do_transition_behaviour(CONF_ON_ENTER_IDLE)
        self.entity.reset_state()

    def on_exit_idle(self):
        self.log.debug("Exiting idle")
        self.do_transition_behaviour(CONF_ON_EXIT_IDLE)

    def on_enter_overridden(self):
        self.log.debug("Entering overridden")
        self.do_transition_behaviour(CONF_ON_ENTER_OVERRIDDEN)
        self._schedule_save_state()

    def on_exit_overridden(self):
        self.log.debug("Exiting overridden")
        self.do_transition_behaviour(CONF_ON_EXIT_OVERRIDDEN)
        self._schedule_save_state()

    def on_enter_active(self, resume=None):
        """resume: seconds to run for a timer restored after a restart
        (restore_active_timer); the lights are then left as they are."""
        self.log.debug("Entering active")
        self.update(last_triggered_at=str(datetime.now()))
        self.backoff_count = 0
        self.prepare_service_data()

        self._start_timer(resume)

        self.log.debug("on_enter_active :: light params before turning on: " + str(self.lightParams))
        if resume is None:
            self.do_transition_behaviour(CONF_ON_ENTER_ACTIVE)
        self.enter()

    def on_exit_active(self):
        self.log.debug("Exiting active")
        self.log.debug("on_exit_active :: Turning off entities, cancelling timer")
        self._cancel_timer()  # cancel previous timer
        self.update(
            delay=self.lightParams.get(CONF_DELAY)
        )  # no need to update immediately
        self.do_transition_behaviour(CONF_ON_EXIT_ACTIVE)

    # May run off the loop (threading.Timer worker, service executor): a single
    # store, the loop-side fire then finds its window gone. No such hook for
    # blocked: a sensor edge re-enters blocked and must not drop a pending check.
    def on_exit_active_timer(self):
        self._debounce_window = None

    def on_exit_active_stay_on(self):
        self._debounce_window = None

    def on_enter_blocked(self):
        self.log.debug("Entering blocked")
        self.update(blocked_at=datetime.now())
        self.update(blocked_by=self._state_entity_state())

        self.do_transition_behaviour(CONF_ON_ENTER_BLOCKED)
        self._block_anchor = self.hass.loop.time()
        if self.block_timeout:
            self._start_block_timer()
        self._schedule_save_state()

    def _start_block_timer(self):
        # Locals only: the expiring timer's worker may clear the handle
        # (on_exit_blocked) at any point in between.
        old, new = self.block_timer_handle, Timer(self.block_timeout, self.block_timer_expire)
        if old is not None:
            old.cancel()
        self.block_timer_handle = new
        self._block_timer_started = self.hass.loop.time()
        new.start()
        self.update(block_timeout=self.block_timeout,
                    expires_at=datetime.now() + timedelta(seconds=self.block_timeout))

    def on_exit_blocked(self):
        self.log.debug("Exiting blocked")
        self.do_transition_behaviour(CONF_ON_EXIT_BLOCKED)
        handle = self.block_timer_handle
        if handle:
            handle.cancel()
        self.block_timer_handle = None
        self._block_ending = True
        self._schedule_save_state()

    def on_enter_constrained(self):
        self.log.debug("Entering constrained")
        self.do_transition_behaviour(CONF_ON_ENTER_CONSTRAINED)

    def on_exit_constrained(self):
        self.log.debug("Exiting constrained")
        self.do_transition_behaviour(CONF_ON_EXIT_CONSTRAINED)
    # =====================================================
    #    C O N F I G U R A T I O N  &  V A L I D A T I O N
    # =====================================================

    def config_transition_behaviours(self, config):
        self.transition_behaviours = {
            CONF_ON_ENTER_IDLE: CONF_TRANSITION_BEHAVIOUR_OFF,          # By default turn off
            CONF_ON_EXIT_IDLE: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_ENTER_ACTIVE: CONF_TRANSITION_BEHAVIOUR_ON,         # By default turn on
            CONF_ON_EXIT_ACTIVE: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_ENTER_OVERRIDDEN: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_EXIT_OVERRIDDEN: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_ENTER_CONSTRAINED: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_EXIT_CONSTRAINED: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_ENTER_BLOCKED: CONF_TRANSITION_BEHAVIOUR_IGNORE,
            CONF_ON_EXIT_BLOCKED: CONF_TRANSITION_BEHAVIOUR_IGNORE,
        }

        if CONF_BEHAVIOURS in config:
            self.transition_behaviours = {**self.transition_behaviours, **config[CONF_BEHAVIOURS]}

        self.log.debug("config_transition_behaviours :: Transition Behaviours: " +  pprint.pformat(self.transition_behaviours))

    def config_control_entities(self, config):
        self.controlEntities = []

        self.add(self.controlEntities, config, CONF_CONTROL_ENTITY)
        self.add(self.controlEntities, config, CONF_CONTROL_ENTITIES)

        self.log.debug("Control Entities: " +  pprint.pformat(self.controlEntities))

    def config_state_entities(self, config):
        self.stateEntities = []
        self.add(
            self.stateEntities, config, CONF_STATE_ENTITIES
        )  # adding optimistically
        self._state_entities_explicit = len(self.stateEntities) > 0
        if len(self.stateEntities) > 0:  # now checking whether they actually exist
            self.log.info(
                "State Entities (explicitly defined - I hope you know what you are doing): " + str(self.stateEntities)
            )
            event.async_track_state_change_event(
                self.hass, self.stateEntities, self.state_entity_state_change
            )

        if len(self.stateEntities) == 0:
            # If no state entities are defined, use control entites as state
            self.stateEntities = self.controlEntities.copy()
            self.log.debug(
                "Added Control Entities as state entities (default): " + str(self.stateEntities)
            )
            event.async_track_state_change_event(
                self.hass, self.stateEntities, self.state_entity_state_change
            )

    def config_off_entities(self, config):

        self.triggerOnDeactivate = []
        self.add(self.triggerOnDeactivate, config, CONF_TRIGGER_ON_DEACTIVATE)
        if len(self.triggerOnDeactivate) > 0:
            self.log.info("Off Entities: " +  pprint.pformat(self.triggerOnDeactivate))

    def config_on_entities(self, config):
        self.triggerOnActivate = []
        self.add(self.triggerOnActivate, config, CONF_TRIGGER_ON_ACTIVATE)
        if len(self.triggerOnActivate) > 0:
            self.log.info("On Entities: " +  pprint.pformat(self.triggerOnActivate))

    def config_sensor_entities(self, config):
        self.sensorEntities = []
        self.add(self.sensorEntities, config, CONF_SENSOR)
        self.add(self.sensorEntities, config, CONF_SENSORS)

        if len(self.sensorEntities) == 0 and len(config.get(CONF_FORCED_SENSORS, [])) == 0 \
                and len(config.get(CONF_EVENT_SENSORS, [])) == 0:
            self.log.error(
                "No sensor entities defined. You must define at least one sensor entity."
            )

        self.log.debug("Sensor Entities: " +  pprint.pformat(self.sensorEntities))

        if self.sensorEntities:
            event.async_track_state_change_event(
                self.hass, self.sensorEntities, self.sensor_state_change
            )

    def config_hold_sensor_entities(self, config):
        """Sensors that never switch the light on, but keep it on.

        Rationale: "trigger from PIR, hold from PIR + radar". A radar listed
        in `sensors:` would switch the light on by itself (and flicker it
        needlessly while flapping), so it gets its own category: it counts
        towards `_sensor_entity_state()` (thus holding the timer via
        is_sensor_on()), but its on-edge never triggers activation.
        """
        self.holdSensorEntities = []
        self.hold_max_seconds = config.get(CONF_HOLD_MAX_SECONDS, 7200)
        self.add(self.holdSensorEntities, config, CONF_HOLD_SENSORS)
        if self.holdSensorEntities:
            self.log.debug("Hold Sensor Entities: %s", pprint.pformat(self.holdSensorEntities))
            event.async_track_state_change_event(
                self.hass, self.holdSensorEntities, self.hold_sensor_state_change
            )

    def config_forced_sensor_entities(self, config):
        """Phase 3: Configure forced sensors that bypass blocked/constrained/overridden.

        Forced sensors trigger ``force_activate`` instead of ``sensor_on``, so
        the controller transitions directly to the ``active`` state from any
        state (including ``blocked``, ``constrained``, and ``overridden``).
        """
        self.forcedSensorEntities = []
        self.add(self.forcedSensorEntities, config, CONF_FORCED_SENSORS)
        if self.forcedSensorEntities:
            self.log.debug("Forced Sensor Entities: %s", pprint.pformat(self.forcedSensorEntities))
            event.async_track_state_change_event(
                self.hass, self.forcedSensorEntities, self.forced_sensor_state_change
            )

    def config_event_sensors(self, config):
        """Phase 6: Configure HA bus event sensors.

        Each entry in ``event_sensors`` is a HA bus event type string.  When
        that event fires the controller treats it the same way as a sensor
        turning on (transitions to active from idle/active_timer/blocked).
        """
        self.eventSensorTypes = []
        for cancel in self._event_sensor_cancel_callbacks:
            cancel()
        self._event_sensor_cancel_callbacks = []

        raw = config.get(CONF_EVENT_SENSORS, [])
        if not raw:
            return

        for event_type in raw:
            self.eventSensorTypes.append(event_type)
            self.log.debug("config_event_sensors :: Subscribing to HA bus event '%s'", event_type)
            cancel = self.hass.bus.async_listen(
                event_type,
                functools.partial(self.ha_event_sensor_callback, event_type),
            )
            self._event_sensor_cancel_callbacks.append(cancel)

    def config_static_strings(self, config):
        DEFAULT_ON = ["on", "playing", "home", "True"]
        DEFAULT_OFF = ["off", "idle", "paused", "away", "False"]
        self.CONTROL_ON_STATE = config.get("control_states_on", DEFAULT_ON)
        self.CONTROL_OFF_STATE = config.get("control_states_off", DEFAULT_OFF)
        self.SENSOR_ON_STATE = config.get("sensor_states_on", DEFAULT_ON)
        self.SENSOR_OFF_STATE = config.get("sensor_states_off", DEFAULT_OFF)
        self.OVERRIDE_ON_STATE = config.get("override_states_on", DEFAULT_ON)
        self.OVERRIDE_OFF_STATE = config.get("override_states_off", DEFAULT_OFF)
        self.STATE_ON_STATE = config.get("state_states_on", DEFAULT_ON)
        self.STATE_OFF_STATE = config.get("state_states_off", DEFAULT_OFF)

        on = config.get("state_strings_on", False)
        if on:
            self.CONTROL_ON_STATE.extend(on)
            self.CONTROL_ON_STATE.extend(on)
            self.SENSOR_ON_STATE.extend(on)
            self.OVERRIDE_ON_STATE.extend(on)
            self.STATE_ON_STATE.extend(on)

        off = config.get("state_strings_off", False)
        if off:
            self.CONTROL_OFF_STATE.extend(off)
            self.SENSOR_OFF_STATE.extend(off)
            self.OVERRIDE_OFF_STATE.extend(off)
            self.STATE_OFF_STATE.extend(off)

    def config_night_mode(self, config):
        """
            Configured night mode parameters. If no night_mode service
            parameters are given, the day mode parameters are used instead.
            If those do not exist, the
        """
        if "night_mode" in config:
            self.night_mode = config[CONF_NIGHT_MODE]
            night_mode = config[CONF_NIGHT_MODE]
            self.light_params_night[CONF_DELAY] = night_mode.get(
                CONF_DELAY, config.get(CONF_DELAY, DEFAULT_DELAY)
            )
            self.light_params_night[CONF_SERVICE_DATA] = night_mode.get(
                CONF_SERVICE_DATA, self.light_params_day.get(CONF_SERVICE_DATA)
            )
            self.light_params_night[CONF_SERVICE_DATA_OFF] = night_mode.get(
                CONF_SERVICE_DATA_OFF, self.light_params_day.get(CONF_SERVICE_DATA_OFF)
            )

            self.night_mode_entity = night_mode.get(CONF_NIGHT_MODE_ENTITY)
            self.night_mode_entity_states = night_mode.get(
                CONF_NIGHT_MODE_ENTITY_STATES, []
            )
            self.nightControlEntities = night_mode.get(CONF_NIGHT_MODE_ENTITIES, [])

            has_window = "start_time" in night_mode and "end_time" in night_mode
            if self.night_mode_entity is not None and not self.night_mode_entity_states:
                self.log.error(
                    "Night mode 'entity' requires 'entity_states' (list of states that mean night). Disabling night mode."
                )
                self.night_mode = None
                self.night_mode_entity = None
                self.nightControlEntities = []
                return
            if self.night_mode_entity is None and not has_window:
                self.log.error(
                    "Night mode requires either start_time+end_time or entity+entity_states! Disabling night mode."
                )
                self.night_mode = None
                self.nightControlEntities = []
                return

            if self.nightControlEntities:
                self.log.info(
                    "Night Control Entities: %s", str(self.nightControlEntities)
                )
                # observe night targets like any other state entity so a
                # manually-lit night light blocks activation and external
                # changes are noticed — but only when state entities were
                # defaulted from control entities, never when set explicitly
                if not self._state_entities_explicit:
                    new_watch = [
                        e for e in self.nightControlEntities
                        if e not in self.stateEntities
                    ]
                    if new_watch:
                        self.stateEntities.extend(new_watch)
                        event.async_track_state_change_event(
                            self.hass, new_watch, self.state_entity_state_change
                        )
                        self.log.debug(
                            "Added night control entities as state entities: %s",
                            str(new_watch),
                        )

    def config_state_attributes_ignore(self, config):
        self.add(self.ignored_event_sources, config, CONF_IGNORED_EVENT_SOURCES)
        self.add(self.state_attributes_ignore, config, CONF_STATE_ATTRIBUTES_IGNORE)
        self.log.debug(
            "Ignoring events (state changes) caused by the following entities): %s",
            self.ignored_event_sources,
        )
        self.log.debug(
            "Ignoring state changes on the following attributes: %s",
            self.state_attributes_ignore,
        )

    def config_normal_mode(self, config):
        self.log.info("Service data set up")
        params = {}
        params[CONF_DELAY] = config.get(CONF_DELAY, DEFAULT_DELAY)
        params[CONF_SERVICE_DATA] = config.get(CONF_SERVICE_DATA, None)
        params[CONF_SERVICE_DATA_OFF] = config.get(CONF_SERVICE_DATA_OFF, None)
        self.light_params_day = params

    @property
    def start_time(self):
        """ Wrapper for _start_time_private """
        return self.debug_time_wrapper(self._start_time_private)

    @property
    def end_time(self):
        """ Wrapper for _end_time_private """
        return self.debug_time_wrapper(self._end_time_private)

    def config_times(self, config):
        self._start_time_private = None
        self._end_time_private = None
        if CONF_START_TIME in config and CONF_END_TIME in config:
            # FOR OPTIONAL DEBUGGING: for initial setup use the raw input value
            self._start_time_private = config.get(CONF_START_TIME)
            self._end_time_private = config.get(CONF_END_TIME)
            start_time_parsed = self.parse_time(self.start_time)
            self.log.debug("start_time_parsed: %s", start_time_parsed)

            self.log.debug("futurize outputs %s", self.futurize(start_time_parsed))

            parsed_start = self.parse_time(self.start_time, aware=False)
            parsed_end = self.parse_time(self.end_time, aware=False)
            # parsed_start = datetime.now() + timedelta(seconds=5)
            # parsed_end = datetime.now() + timedelta(seconds=10)
            # FOR OPTIONAL DEBUGGING: subsequently use normal delay
            sparts = re.search(r"^(now\s*[+-]\s*\d+)", config.get(CONF_START_TIME))
            if sparts is not None:
                self._start_time_private = sparts.group(1)
            eparts = re.search(r"^(now\s*[+-]\s*\d+)", config.get(CONF_END_TIME))
            if eparts is not None:
                self._end_time_private = eparts.group(1)

            self.update(start=self.start_time)
            self.update(end=self.end_time)

            parsed_start = self.futurize(parsed_start)
            parsed_end = self.futurize(parsed_end)
            self.log.debug("Setting FIRST START callback for %s", parsed_start)
            self.log.debug("Setting FIRST END callback for %s", parsed_end)

            self.start_time_event_hook = event.async_track_point_in_time(
                self.hass, self.start_time_callback, parsed_start
            )
            self.end_time_event_hook = event.async_track_point_in_time(
                self.hass, self.end_time_callback, parsed_end
            )

            if not self.now_is_between(self.start_time, self.end_time):
                self.log.debug(
                    "Constrain period active. Scheduling transition to 'constrained'"
                )
                event.async_call_later(self.hass, 1, self.constrain_entity)

        self.log_config()




    def config_override_entities(self, config):
        self.overrideEntities = []
        self.add(self.overrideEntities, config, "override")
        self.add(self.overrideEntities, config, "overrides")

        if len(self.overrideEntities) > 0:
            self.log.debug("Override Entities: " +  pprint.pformat(self.overrideEntities))
            event.async_track_state_change_event(
                self.hass, self.overrideEntities, self.override_state_change
            )

    def config_lux_constraint(self, config):
        """Configure the optional illuminance (lux) activation constraint.

        When ``lux_entity`` is set together with ``lux_threshold`` (numeric
        sensors) and/or ``lux_bright_states`` (string bright/dim sensors),
        sensor_on activation from ``idle`` (with the state entities off)
        requires the room to be dark enough. Everything else — timer resets,
        blocked handling, overrides, forced sensors, and the ``activate``
        service — is unaffected, so a running controller keeps managing
        lights whose own output pushes the reading past the threshold, and a
        manual activation always works.
        """
        self.luxEntity = config.get(CONF_LUX_ENTITY, None)
        self.lux_threshold = config.get(CONF_LUX_THRESHOLD, None)
        self.lux_bright_states = config.get(CONF_LUX_BRIGHT_STATES, []) or []
        # seconds before a lux-blocked activation is re-evaluated once (None/0
        # disables); see _schedule_lux_recheck for the ordering race it covers
        delay = config.get(CONF_LUX_RECHECK_DELAY, 1.0)
        try:
            self.lux_recheck_delay = float(delay) if delay else None
        except (TypeError, ValueError):
            self.log.error("lux_recheck_delay must be a number of seconds, got %r; using 1.0", delay)
            self.lux_recheck_delay = 1.0

        if self.luxEntity is not None and self.lux_threshold is None \
                and not self.lux_bright_states:
            self.log.error(
                "lux_entity is set but neither lux_threshold nor lux_bright_states is. "
                "The lux constraint is disabled."
            )
            self.luxEntity = None
        if self.luxEntity is None and (
            self.lux_threshold is not None or self.lux_bright_states
        ):
            self.log.error(
                "lux_threshold/lux_bright_states are set but lux_entity is not. "
                "The lux constraint is disabled."
            )
            self.lux_threshold = None
            self.lux_bright_states = []

        if self.luxEntity is not None:
            if self.lux_threshold is not None:
                self.lux_threshold = float(self.lux_threshold)
            self.log.debug(
                "Lux constraint: %s, threshold %s, bright states %s",
                self.luxEntity,
                self.lux_threshold,
                self.lux_bright_states,
            )
            self.update(
                lux_entity=self.luxEntity,
                lux_threshold=self.lux_threshold,
                lux_bright_states=self.lux_bright_states or None,
            )

    def config_other(self, config):
        self.do_draw = config.get("draw", False)
        self.ignore_state_changes_until = datetime.now()
        self.homeassistant_turn_on_domains = ['group'] # domains that do not have their own turn_on service and rely on homeassistant.turn_on

        self.config[CONF_SENSOR_RESETS_TIMER] = config.get(CONF_SENSOR_RESETS_TIMER)

        self.block_timeout = config.get(CONF_BLOCK_TIMEOUT, None)
        self.grace_period = config.get(CONF_IGNORE_STATE_CHANGES_UNTIL, None)
        raw = config.get(CONF_STATE_CHANGE_DEBOUNCE, DEFAULT_STATE_CHANGE_DEBOUNCE)
        try:
            if isinstance(raw, bool):
                raise ValueError(raw)
            debounce = 0.0 if raw is None else float(raw)
            if not 0 <= debounce <= 60:
                raise ValueError(raw)
        except (TypeError, ValueError, OverflowError):
            self.log.error(
                "state_change_debounce must be 0-60 seconds or null, got %r; using %s",
                raw, DEFAULT_STATE_CHANGE_DEBOUNCE)
            debounce = DEFAULT_STATE_CHANGE_DEBOUNCE
        self.state_change_debounce = debounce or None
        self.disable_block = config.get(CONF_DISABLE_BLOCK, False)
        external_on = config.get(CONF_BLOCK_ON_EXTERNAL_ON, True)
        if not isinstance(external_on, bool):
            self.log.error("block_on_external_on must be true or false, got %r; using true", external_on)
            external_on = True
        self.block_on_external_on = external_on
        self.update(block_on_external_on=external_on)
        if not external_on:
            self.log.info("block_on_external_on is false: a light switched on outside EC while idle is left as it is")
        self.image_prefix = config.get("image_prefix", "/fsm_diagram_")
        self.image_path = config.get("image_path", "/conf/temp")
        self.backoff = config.get("backoff", False)
        self.stay = config.get("stay_mode", False)

        if self.backoff:
            self.log.debug("config_other :: setting up backoff. Using delay as initial backoff value.")
            self.backoff_factor = config.get("backoff_factor", 1.1)
            self.backoff_max = config.get("backoff_max", 300)

        if config.get(CONF_SENSOR_TYPE_DURATION):
            self.sensor_type = SENSOR_TYPE_DURATION
        else:
            self.sensor_type = SENSOR_TYPE_EVENT

        if CONF_SENSOR_TYPE in config:
            self.sensor_type = config.get(CONF_SENSOR_TYPE)

        self.update(sensor_type=self.sensor_type)

    # =====================================================
    #    S T A T E   P E R S I S T E N C E  (Phase 2)
    # =====================================================

    def _storage_key(self):
        """Return a unique storage key for this EC instance."""
        safe_name = re.sub(r'[^a-z0-9_]+', '_', self.name.lower()).strip('_')
        return f"{STORAGE_KEY_PREFIX}{safe_name}"

    def _schedule_save_state(self):
        """Schedule an async state save without blocking the caller.

        Must be thread-safe: ``on_enter_blocked`` arms a ``threading.Timer``
        whose ``block_timer_expire`` callback drives state transitions from a
        worker thread, so the ``on_exit_blocked`` -> ``_schedule_save_state``
        path is reached off the event loop. HA 2026.x raises ``RuntimeError``
        when ``hass.async_create_task`` is called from a non-event-loop thread,
        which aborts the transition and traps the controller in ``blocked``
        until HA restarts.
        """
        if self._store is not None:
            asyncio.run_coroutine_threadsafe(self._async_save_state(), self.hass.loop)

    async def _async_save_state(self, *_args):
        """Persist the current EC state to HA storage."""
        if self._store is None:
            return
        state, expiry = self.state, self._expiry_time
        wait = getattr(self, "_restore_wait", None)
        if wait is not None:
            # Still waiting for the lights of a restored timer: keep that
            # timer for the next start instead of saving 'pending'.
            state, expiry = "active_timer", wait.expiry
        data = {
            "state": state,
            "saved_at": str(datetime.now()),
            # The timer expiry time is needed so the run-out can be finished
            # after a restart — otherwise EC ends up in 'idle' and nothing ever
            # turns the controlled light off (measured 2026-08-19: out of 24
            # restarts the light stayed on 21 times, with a 20-40 min tail
            # after the last motion).
            "expires_at": str(expiry) if expiry else None,
        }
        self.log.debug("_async_save_state :: Saving state: %s", data)
        await self._store.async_save(data)

    async def _async_restore_state(self):
        """Load persisted state from HA storage and restore if applicable.

        Returns True when the state was successfully restored, or when EC
        already left pending while the store loaded (nothing to restore), so
        the caller can skip the normal startup evaluation.
        """
        if self._store is None:
            return False
        try:
            data = await self._store.async_load()
        except (OSError, ValueError) as exc:
            self.log.warning("_async_restore_state :: Failed to load persisted state: %s", exc)
            data = None
        if getattr(self, "state", "pending") != "pending":
            # EC left pending while the store loaded (constrain 1 s after
            # the config, a forced sensor): leave it there; the caller's
            # start_monitoring() would raise MachineError. getattr: test
            # models built with Model.__new__ have no state.
            return True

        if not data:
            self.log.debug("_async_restore_state :: No persisted state found")
            return False

        saved_state = data.get("state")
        self.log.debug("_async_restore_state :: Restoring state '%s' (saved at %s)",
                       saved_state, data.get("saved_at"))

        if saved_state == "overridden":
            # Only restore overridden if an override entity is actually on, so
            # we do not get stuck in overridden when the override cleared while HA
            # was stopped.
            if len(self.overrideEntities) > 0 and self.is_override_state_on():
                self.override()
                self.update(overridden_at=str(datetime.now()), notes="Restored from storage")
                return True
            # Override is gone — fall through to normal startup evaluation
            return False

        if saved_state == "blocked":
            # Only restore blocked if the state entity is still on (otherwise the
            # entity was turned off while HA was down — go to idle instead).
            # Straight from pending (2026-10-31): the old way through idle ran
            # on_enter_idle, which switched every light off and left EC blocked
            # in the dark. set_context() replaces the one on_enter_idle set.
            if len(self.overrideEntities) > 0 and self.is_override_state_on():
                return False  # the caller enters overridden, as on a normal start
            if self.is_state_entities_on() and self.is_block_enabled():
                self.set_context(None)
                self.restore_blocked()
                return True
            return False

        if saved_state == "active_timer":
            # The timer was running when HA shut down. Without restoring it EC
            # ends up in 'idle' and nothing turns the controlled light off —
            # only new motion completes the cycle. Measured 2026-08-19 in
            # production: out of 24 restarts the light stayed on 21 times, with
            # a 20-40 min tail after the last motion instead of the expected
            # ~5.5 min (90 s hold + 240 s delay).
            expiry = self._parse_saved_expiry(data.get("expires_at"))
            if expiry is None:
                return False
            return self._restore_timer(expiry)

        # For all other states (idle, constrained, etc.) let the normal startup
        # evaluation run.
        return False

    def _restore_timer(self, expiry, settled=False):
        """Settle a saved active_timer from the live states (EC in pending).

        Runs at the restore and again on each comeback while the restore
        waits for its lights. Returns False only at the restore, when the
        caller starts as usual (an override is on, or every light is off).
        EC is in pending: the restore checks it after the store load, and a
        wait ends on every exit from pending (on_exit_pending).
        settled: the lights stayed off for state_change_debounce after
        coming back (_restore_wait_settled).
        """
        waiting = self._restore_wait is not None
        override = self._restore_override_on()
        if override is not None:
            if not waiting:
                return False  # the caller enters overridden, as on a normal start
            self.set_context(None)
            self.update(overridden_by=override)
            self.override()
            self.update(overridden_at=str(datetime.now()),
                        notes="Override turned on while waiting for the lights after the restart")
            return True
        lit, unreachable = self._restore_light_states()
        if not lit:
            if unreachable:
                # No reachable light is on: a turn-off would go nowhere and a
                # resume would let the comeback resync light a dark room, so
                # wait for the others.
                if not waiting:
                    self._restore_wait_start(expiry, unreachable)
                return True
            if not waiting:
                return False  # every light is off: nothing to finish
            if self.state_change_debounce and not settled:
                # Tasmota lights come back 'off' and report their real state
                # 1-2 s later: end the wait only once every light has stayed
                # off that long (_restore_wait_event keeps listening).
                self._restore_settle(self._restore_wait)
                return True
            self.log.info("_restore_timer :: the lights came back off; idle without turning anything off")
            self._restore_idle("Lights came back off after the restart; idle without turning anything off")
            return True
        switched_on = sorted(self._restore_wait.switched_on.intersection(lit)) if waiting else []
        if switched_on:
            if self._restore_block_switched_on(switched_on):
                return True
            # Someone switched a light on during the wait: external control,
            # as at runtime; the restore never switches such a light off.
            self.log.info("_restore_timer :: %s switched on during the wait; idle without turning anything off",
                          ", ".join(switched_on))
            self._restore_idle("%s switched on during the wait after the restart; idle without turning "
                               "anything off" % ", ".join(switched_on))
            return True
        self.prepare_service_data()  # day/night delay and targets as of now
        sensor_on, sensor_unknown = self._restore_sensor_knowledge()
        delay, cap = self._restore_delays()
        remaining = (expiry - datetime.now()).total_seconds()
        if remaining <= 1 and not (sensor_on or sensor_unknown):
            # Ran out while HA was down and every sensor reports off: one
            # turn-off, whatever on_enter_idle says.
            self.log.info(
                "_restore_timer :: timer ran out while HA was down (%s) and no sensor "
                "holds the light, turning control entities off", expiry)
            self.start_monitoring(quiet=True)
            self.turn_off_control_entities()
            self.update(notes="Timer ran out while HA was down and no sensor holds the light — entities turned off")
            return True
        if remaining <= 1:
            # A sensor holds the light, or has not reported yet (Zigbee2MQTT
            # sensors stay 'unknown' for 15 s to minutes after a start): run
            # the configured delay, as after the sensor's off edge.
            resume, reason = delay, "ran out while HA was down; a sensor holds the light or has not reported yet"
        elif sensor_unknown and remaining < delay:
            resume, reason = delay, "a sensor has not reported yet; the delay instead of %.0f s left" % remaining
        elif remaining > cap:
            resume, reason = cap, "capped from %.0f s left" % remaining
        else:
            resume, reason = remaining, "time left"
        self.log.info("_restore_timer :: resuming the timer from before the restart for %.1f s (%s)", resume, reason)
        self.set_context(None)
        self.restore_active_timer(resume=resume)
        self.update(notes="Timer from before restart: %.0f s (%s)" % (resume, reason))
        return True

    def _restore_light_states(self):
        """(reachable state entities that are on, state entities not reachable yet).

        Not reachable: unavailable, unknown or not created yet.
        """
        lit, unreachable = [], []
        for e in self.stateEntities:
            s = self.hass.states.get(e)
            if s is None or s.state in (STATE_UNAVAILABLE, STATE_UNKNOWN):
                unreachable.append(e)
            elif self.matches(s.state, self.STATE_ON_STATE):
                lit.append(e)
        return lit, unreachable

    def _restore_override_on(self):
        """The override entity that is on, or None. One not created yet reads
        as off, without the "does not exist (yet)" ERROR of
        _override_entity_state at the restore and at every wait event."""
        for e in self.overrideEntities:
            s = self.hass.states.get(e)
            if s is not None and self.matches(s.state, self.OVERRIDE_ON_STATE):
                return e
        return None

    def _restore_sensor_knowledge(self):
        """(a sensor or hold sensor holds the light, one has not reported yet).

        Not reported: unknown, unavailable or not created yet. Forced and
        HA-event sensors have no state to wait for. Same reading as
        _sensor_entity_state (a stuck hold sensor does not hold), without
        its "does not exist (yet)" ERROR for a sensor not created yet.
        """
        regular = list(self.sensorEntities)
        holds = list(getattr(self, "holdSensorEntities", []))
        on = unknown = False
        for e in regular + holds:
            s = self.hass.states.get(e)
            if s is None or s.state in (STATE_UNAVAILABLE, STATE_UNKNOWN):
                unknown = True
            elif self.matches(s.state, self.SENSOR_ON_STATE) and not (
                    e in holds and e not in regular and self._hold_sensor_is_stuck(e, s)):
                on = True
        return on, unknown

    def _restore_delays(self):
        """(delay of the current mode, cap) in seconds for a restored timer.

        cap is the longest timer this instance can run: the day delay, the
        night delay with night_mode set, backoff_max with backoff on. The
        saved run-out is naive local time, so a DST fall-back or a clock
        step could otherwise resume up to an hour. A value that is not a
        number is left out.
        """
        def seconds(value):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        candidates = [seconds(self.light_params_day.get(CONF_DELAY, DEFAULT_DELAY))]
        if self.night_mode is not None:
            candidates.append(seconds(self.light_params_night.get(CONF_DELAY, DEFAULT_DELAY)))
        if self.backoff:
            candidates.append(seconds(getattr(self, "backoff_max", None)))
        cap = max((c for c in candidates if c is not None), default=float(DEFAULT_DELAY))
        delay = seconds(self.lightParams.get(CONF_DELAY, DEFAULT_DELAY))
        return (cap if delay is None else min(delay, cap)), cap

    def _restore_wait_start(self, expiry, unreachable):
        """Wait in pending for the lights of a restored timer."""
        wait = _RestoreWait(expiry)
        self._restore_wait = wait
        wait.unsubs.append(event.async_track_state_change_event(
            self.hass, list(self.stateEntities) + list(self.overrideEntities),
            functools.partial(self._restore_wait_event, wait)))
        wait.unsubs.append(event.async_call_later(
            self.hass, RESTORE_WAIT_SECONDS,
            functools.partial(self._restore_wait_timeout, wait)))
        self.log.info(
            "_restore_timer :: %s not reachable yet and no reachable light is on; waiting "
            "up to %d s (timer from before restart, expiry %s)",
            ", ".join(unreachable), RESTORE_WAIT_SECONDS, expiry)
        self.update(notes="Waiting up to %d s for %s to report (timer from before restart)"
                    % (RESTORE_WAIT_SECONDS, ", ".join(unreachable)))

    @callback
    def _restore_wait_event(self, wait, ev):
        """A light or override entity changed while a restored timer waits."""
        if self._restore_wait is not wait or self.hass.is_stopping:
            return
        entity = ev.data["entity_id"]
        old, new = ev.data["old_state"], ev.data["new_state"]
        if entity not in self.overrideEntities:
            # A comeback counts (from unavailable, unknown or not created yet
            # to a real state), and so does the light turning on within
            # state_change_debounce of its comeback: its real state after a
            # placeholder 'off'. Any other turn on is someone switching it on.
            # The runtime resync is unchanged.
            not_yet = (STATE_UNAVAILABLE, STATE_UNKNOWN)
            if new is None or new.state in not_yet:
                return
            now = self.hass.loop.time()
            if old is None or old.state in not_yet:
                wait.returned[entity] = now
            elif new.state == old.state or not self.matches(new.state, self.STATE_ON_STATE):
                return
            elif entity not in wait.returned or now - wait.returned[entity] > (self.state_change_debounce or 0):
                wait.switched_on.add(entity)
                return
        self.log.info("_restore_wait_event :: %s is now '%s', settling the timer from before restart",
                      entity, getattr(new, "state", None))
        self._restore_timer(wait.expiry)

    @callback
    def _restore_wait_timeout(self, wait, _now=None):
        """The lights of a restored timer did not report in time."""
        if self._restore_wait is not wait or self.hass.is_stopping or not self.is_pending():
            return
        self.log.warning(
            "_restore_wait_timeout :: lights still not reachable after %d s; ending the wait "
            "without a turn-off", RESTORE_WAIT_SECONDS)
        lit, _ = self._restore_light_states()
        if self._restore_block_switched_on(sorted(wait.switched_on.intersection(lit))):
            return
        self._restore_idle("Lights did not report within %d s after the restart; idle without "
                           "turning anything off" % RESTORE_WAIT_SECONDS)

    def _restore_settle(self, wait):
        """(Re)arm the re-check that ends the wait once the lights stayed off."""
        if wait.settle is not None:
            wait.settle()
        wait.settle = event.async_call_later(
            self.hass, self.state_change_debounce, functools.partial(self._restore_wait_settled, wait))
        self.log.info("_restore_timer :: the lights came back off; idle unless one turns on within %s s",
                      self.state_change_debounce)

    @callback
    def _restore_wait_settled(self, wait, _now=None):
        """The lights stayed off for state_change_debounce after coming back."""
        if self._restore_wait is not wait or self.hass.is_stopping or not self.is_pending():
            return
        wait.settle = None
        self._restore_timer(wait.expiry, settled=True)

    def _restore_block_switched_on(self, lights):
        """Lights switched on during the wait end it in blocked, as at
        runtime in idle (2026-11-10); False when that does not apply."""
        if not lights or not self._external_on_blocks():
            return False
        names = ", ".join(lights)
        self.log.info("_restore_timer :: %s switched on during the wait; blocked", names)
        self.set_context(None)
        self.restore_blocked()
        self.update(last_triggered_by=lights[0],
                    notes="%s switched on during the wait after the restart; blocked" % names)
        return True

    def _restore_idle(self, notes):
        """End a restored timer's wait in idle without a turn-off.

        A sensor edge in pending was dropped (sensor_state_change acts only
        in idle/active_timer/blocked), so a regular sensor that is on now
        gets the sensor_on() its edge would have made: lux-gated
        idle -> active, or blocked when a light was switched on by hand.
        Hold sensors never switch the lights on.
        """
        self.start_monitoring(quiet=True)
        self.update(notes=notes)
        for e in self.sensorEntities:
            s = self.hass.states.get(e)
            if s is not None and self.matches(s.state, self.SENSOR_ON_STATE):
                self.log.info("_restore_idle :: %s is on, acting on it as on its edge", e)
                self.update(last_triggered_by=e, notes=notes + "; %s is on, activating" % e)
                self.sensor_on()
                if self.is_idle() and getattr(self, "_lux_blocked", False):
                    self._schedule_lux_recheck(e)
                break

    def _restore_wait_end(self):
        """Drop a restored timer's wait. Every exit from pending runs on the loop."""
        wait = getattr(self, "_restore_wait", None)
        if wait is None:
            return
        self._restore_wait = None
        for unsub in wait.unsubs + ([wait.settle] if wait.settle else []):
            unsub()
        # Overwritten by the restore's own outcome; stays when an override,
        # constrain or a forced sensor ended the wait.
        self.update(notes="Restore wait ended before the lights reported")

    def _parse_saved_expiry(self, raw):
        """Parse the saved expiry time into a datetime (naive local time)."""
        if not raw or raw == "None":
            return None
        try:
            return datetime.fromisoformat(str(raw))
        except ValueError:
            self.log.warning("_parse_saved_expiry :: Cannot read the saved expires_at: %s", raw)
            return None

    @callback
    def constrain_entity(self, evt):
        """
            Event callback used on component setup if current time requires entity to start in constrained state.
        """
        self.constrain()

    @callback
    def end_time_callback(self, evt):
        """
            Called when `end_time` is reached, will change state to `constrained` and schedule `start_time` callback.
        """
        self.log.debug("end_time_callback :: Triggered")
        # must be reparsed to get up to date sunset/sunrise times
        x = self.parse_time(self.end_time)

        parsed_end = self.futurize(x)
        self.log.debug("end_time_callback :: New callback set to %s (future)", parsed_end)
        self.end_time_event_hook = event.async_track_point_in_time(
            self.hass, self.end_time_callback, parsed_end
        )
        self.update(end_time=parsed_end)

        # must be down here to make sure new callback is set regardless of exceptions
        self.do_transition_behaviour(CONF_ON_ENTER_CONSTRAINED)
        self.constrain()

    @callback
    def start_time_callback(self, evt):
        """
            Called when `start_time` is reached, will change state to `idle` and schedule `end_time` callback.
        """
        self.log.debug("start_time_callback :: Triggered")
        # must be reparsed to get up to date sunset/sunrise times
        # if self.debug_day_length:
        #     x = self.make_naive(dt.now() + timedelta(seconds=int(self.debug_day_length)))
        #     self.log.debug("using debug day lengh %s", x)
        # else:
        x = self.parse_time(self.start_time)

        parsed_start = self.futurize(x)
        self.log.debug(
            "start_time_callback :: New callback set to %s (future)" % parsed_start
        )
        self.start_time_event_hook = event.async_track_point_in_time(
            self.hass, self.start_time_callback, parsed_start
        )

        self.update(start_time=parsed_start)

        self._apply_start_time_transition()
        self.do_transition_behaviour(CONF_ON_EXIT_CONSTRAINED)

    def _apply_start_time_transition(self):
        """Transition the machine when start_time is reached.

        Must never raise: an exception escaping a point-in-time callback makes
        HA re-fire it endlessly (error storm, 2026-07-08). States with no
        defined transition (e.g. overridden or active after a mid-window
        restart) are logged and left unchanged — the next sensor/override
        event resolves the controller normally.
        """
        try:
            # Override-at-start_time fix (2026-07-30): an override that switched
            # on while we were still constrained never reached the machine.
            # override_state_change()
            # only calls override() from active/active_timer/idle/blocked --
            # matching the `override` trigger, which has no `constrained`
            # source -- so the event is dropped on the floor.  Without this
            # branch the blocked() path below then strands the controller:
            # blocked ignores the override entirely, and when the block clears
            # (block_timer_expires, or enable() once the state entities go off)
            # the machine lands in idle = armed, with the override forgotten.
            # Observed 2026-07-29 on ec_202: the night block was armed 6s after
            # start_time (= sunset) while the controlled light happened to be
            # on, and the room then lit up on presence 7 times during the night.
            # enable() has a dedicated constrained -> overridden transition, so
            # routing through it honours the override without a new transition.
            # Scoped to `constrained` on purpose: start_time_callback also runs
            # while already idle/blocked/etc. (see the Phase 4 comment above),
            # and those states are reached only after override_state_change()
            # has had its chance, so they must keep the original behaviour.
            if (
                self.is_constrained()
                and len(self.overrideEntities) > 0
                and self.is_override_state_on()
            ):
                self.update(overridden_by=self._override_entity_state())
                self.enable()
                self.update(overridden_at=str(datetime.now()))
            elif self.is_state_entities_on() and self.is_block_enabled():
                self.blocked()
            else:
                # If the entity is on and block is disabled, we just transition from constrained
                # to idle and leave the entity on. (Don't start a timer to turn it off.)
                self.enable()
        except MachineError as err:
            self.log.warning(
                "start_time_callback :: no transition from state '%s' (%s); leaving state unchanged",
                self.state,
                err,
            )

    # =====================================================
    #    H E L P E R   F U N C T I O N S        ( N E W )
    # =====================================================

    def handleTriggerOnDeactivateEntities(self):
        """ Entities that are defined outside of control entities via the `triggerOnDetivate` key. """
        if len(self.triggerOnDeactivate) > 0:
            self.log.info("handleTriggerOnDeactivateEntities :: Triggering Deactivation entities (Yes! params passed along)")
            for e in self.triggerOnDeactivate:
                self.log.debug("Triggering with turn_on call: %s", e)
                if self.lightParams.get(CONF_SERVICE_DATA_OFF) is not None:
                    self.log.debug(
                        "Triggering with turn_on call %s with service parameters %s",
                        e,
                        self.lightParams.get(CONF_SERVICE_DATA_OFF),
                    )
                    self.call_service(e, "turn_on", **self.lightParams.get(CONF_SERVICE_DATA_OFF))
                else:
                    self.log.debug("Triggering with turn_on call %s (no parameters provided to pass to service call)", e)
                    self.call_service(e, "turn_on")

    def handleTriggerOnActivateEntities(self):
        """ Entities that are defined outside of control entities via the `triggerOnActivate` key. """
        if len(self.triggerOnActivate) > 0:
            self.log.info("handleTriggerOnActivateEntities :: Triggering Activation entities (Yes! params passed along)")
            for e in self.triggerOnActivate:
                # if light params are defined
                if self.lightParams.get(CONF_SERVICE_DATA) is not None:
                    self.log.debug(
                        "Triggering with turn_on call %s with service parameters %s",
                        e,
                        self.lightParams.get(CONF_SERVICE_DATA),
                    )
                    self.call_service(
                        e, "turn_on", **self.lightParams.get(CONF_SERVICE_DATA)
                    )
                else:
                    self.log.debug("Turning on %s (no parameters provided to pass to service call)", e)
                    self.call_service(e, "turn_on")

    def turn_on_control_entities(self):
        self.handleTriggerOnActivateEntities()

        for e in self.activeControlEntities or self.controlEntities:
            # if light params are defined
            if self.lightParams.get(CONF_SERVICE_DATA) is not None:
                self.log.debug(
                    "turn_on_control_entities :: Turning on %s with service parameters %s",
                    e,
                    self.lightParams.get(CONF_SERVICE_DATA),
                )
                self.call_service(
                    e, "turn_on", **self.lightParams.get(CONF_SERVICE_DATA)
                )
            else:
                self.log.debug(
                    "turn_on_control_entities :: Turning on %s (no parameters passed to service call)", e
                )
                self.call_service(e, "turn_on")

    def turn_off_control_entities(self):
        self.handleTriggerOnDeactivateEntities()
        targets = list(self.activeControlEntities or self.controlEntities)
        if getattr(self, "_block_ending", False) and self.is_idle():
            # 2026-11-10: a block ending in idle also switches off a lit target of the other mode
            targets += [e for e in dict.fromkeys(self.controlEntities + self.nightControlEntities)
                        if e not in targets and self._is_control_on(e)]
        for e in targets:
            self.log.debug("turn_off_control_entities :: Turning off %s", e)

            if self.lightParams.get(CONF_SERVICE_DATA_OFF) is not None:
                self.call_service(
                    e, "turn_off", **self.lightParams.get(CONF_SERVICE_DATA_OFF)
                )
            else:
                self.call_service(e, "turn_off")

    def now_is_between(self, start_time_str, end_time_str, name=None):
        start_time = (self._parse_time(start_time_str, name))["datetime"]
        end_time = (self._parse_time(end_time_str, name))["datetime"]
        now = dt.as_local(dt.now())
        start_date = now.replace(
            hour=start_time.hour, minute=start_time.minute, second=start_time.second
        )
        end_date = now.replace(
            hour=end_time.hour, minute=end_time.minute, second=end_time.second
        )
        if end_date < start_date:
            # Spans midnight
            if now < start_date and now < end_date:
                now = now + timedelta(days=1)
            end_date = end_date + timedelta(days=1)

        self.log.debug("now_is_between start time %s", start_date)
        self.log.debug("now_is_between end time %s", end_date)
        return start_date <= now <= end_date

    def parse_time(self, time_str, name=None, aware=False):
        if aware is True:
            return dt.as_local(self._parse_time(time_str, name)["datetime"]).time()
        else:
            return self.make_naive(
                (self._parse_time(time_str, name))["datetime"]
            ).time()

    def parse_datetime(self, time_str, name=None, aware=False):
        if aware is True:
            return dt.as_local(self._parse_time(time_str, name)["datetime"])
        else:
            return self.make_naive(
                dt.as_local(self._parse_time(time_str, name)["datetime"])
            )

    def _parse_time(self, time_str, name=None):
        parsed_time = None
        sun = None
        offset = 0
        parts = re.search(r"^(\d+)-(\d+)-(\d+)\s+(\d+):(\d+):(\d+)$", str(time_str))
        if parts:
            this_time = datetime(
                int(parts.group(1)),
                int(parts.group(2)),
                int(parts.group(3)),
                int(parts.group(4)),
                int(parts.group(5)),
                int(parts.group(6)),
                0,
            )
            parsed_time = dt.as_local(this_time)
        else:
            parts = re.search(r"^(\d+):(\d+):(\d+)$", str(time_str))
            if parts:
                today = dt.as_local(dt.now())
                time_temp = time(
                    int(parts.group(1)), int(parts.group(2)), int(parts.group(3)), 0
                )
                parsed_time = today.replace(
                    hour=time_temp.hour,
                    minute=time_temp.minute,
                    second=time_temp.second,
                    microsecond=0,
                )

            else:
                if time_str == "sunrise":
                    parsed_time = self.sunrise(True)
                    sun = "sunrise"
                    offset = 0
                elif time_str == "sunset":
                    parsed_time = self.sunset(True)
                    sun = "sunset"
                    offset = 0
                else:
                    parts = re.search(
                        r"^sunrise\s*([+-])\s*(\d+):(\d+):(\d+)$", str(time_str)
                    )
                    if parts:

                        sun = "sunrise"
                        if parts.group(1) == "+":
                            td = timedelta(
                                hours=int(parts.group(2)),
                                minutes=int(parts.group(3)),
                                seconds=int(parts.group(4)),
                            )
                            offset = td.total_seconds()
                            parsed_time = self.sunrise(True) + td
                        else:
                            td = timedelta(
                                hours=int(parts.group(2)),
                                minutes=int(parts.group(3)),
                                seconds=int(parts.group(4)),
                            )
                            offset = td.total_seconds() * -1
                            parsed_time = self.sunrise(True) - td
                    else:
                        parts = re.search(
                            r"^sunset\s*([+-])\s*(\d+):(\d+):(\d+)$", str(time_str)
                        )
                        if parts:
                            sun = "sunset"
                            if parts.group(1) == "+":
                                td = timedelta(
                                    hours=int(parts.group(2)),
                                    minutes=int(parts.group(3)),
                                    seconds=int(parts.group(4)),
                                )
                                offset = td.total_seconds()
                                parsed_time = self.sunset(True) + td
                            else:
                                td = timedelta(
                                    hours=int(parts.group(2)),
                                    minutes=int(parts.group(3)),
                                    seconds=int(parts.group(4)),
                                )
                                offset = td.total_seconds() * -1
                                parsed_time = self.sunset(True) - td
        if parsed_time is None:
            if name is not None:
                raise ValueError("%s: invalid time string: %s", name, time_str)
            else:
                raise ValueError("invalid time string: %s", time_str)
        # self.log.debug("Result of parsing: %s",
        #                {"datetime": parsed_time, "sun": sun, "offset": offset})
        return {"datetime": parsed_time, "sun": sun, "offset": offset}

    def make_naive(self, dts):
        local = dt.as_local(dts)
        return datetime(
            local.year,
            local.month,
            local.day,
            local.hour,
            local.minute,
            local.second,
            local.microsecond,
        )

    def sunset(self, aware):
        t = get_astral_event_date(
            self.hass, SUN_EVENT_SUNSET, datetime.now().replace(hour=0)
        )
        if aware is True:
            return dt.as_local(t)
        else:
            return t

    def sunrise(self, aware):
        t = get_astral_event_date(
            self.hass, SUN_EVENT_SUNRISE, datetime.now().replace(hour=0)
        )
        if aware is True:
            return dt.as_local(t)
        else:
            return t

    def next_sunrise(self, offset=0):
        mod = offset
        while True:

            next_rising_dt = self.sunrise(True) + timedelta(mod)
            if next_rising_dt > dt.now():
                break

            mod += 1

        return next_rising_dt

    def next_sunset(self, offset=0):
        mod = offset
        while True:

            next_setting_dt = self.sunset(True) + timedelta(mod)
            if next_setting_dt > dt.now():
                break

            mod += 1

        return next_setting_dt

    # =====================================================
    #    H E L P E R   F U N C T I O N S    ( C U S T O M )
    # =====================================================

    # def adjust_times(self, start, end):
    #     """ Makes sure that a time period is in the future. """
    #     self.log.debug("Parsed start time (unadjusted) %s", start)
    #     self.log.debug("Parsed end time (unadjusted) %s", end)
    #
    #     # set start_time callback: if time passed, use tomorrow
    #     # --------s--------n----|12am|--s(new)--------
    #     #         \_________>>>________/
    #     now = datetime.now()
    #     if start <= now:
    #         start += timedelta(1)  # start time is tomorrow!
    #
    #     # we now need parsed_end to come after the new parse_start
    #     # (1) ---s---e---now
    #     # (2) ---e---s---now
    #     if end <= now:
    #         end += timedelta(1)  # (1)
    #     # if end <= start:
    #     # bump again because its still before s
    #     #    end += timedelta(1)  # (2)
    #     return start, end

    def prepare_service_data(self):
        """
            Called when entering active state and on initial set up to set
            correct service parameters.
        """
        if self.is_night():
            self.log.debug(
                "Using NIGHT MODE parameters: " + str(self.light_params_night)
            )
            self.lightParams = self.light_params_night
            self.activeControlEntities = (
                self.nightControlEntities or self.controlEntities
            )
            self.update(mode=MODE_NIGHT)
        else:
            self.log.debug("Using DAY MODE parameters: " + str(self.light_params_day))
            self.lightParams = self.light_params_day
            self.activeControlEntities = self.controlEntities
            if self.night_mode is not None:
                self.update(mode=MODE_DAY)  # only show when night mode set up
        if self.nightControlEntities:
            self.update(active_entities=list(self.activeControlEntities))
        self.update(delay=self.lightParams.get(CONF_DELAY))

    def call_service(self, entity, service, **service_data):
        """ Helper for calling HA services with the correct parameters """
        self.log.debug("call_service :: Calling service " + service + " on " + entity)
        if self.grace_period:
            self.ignore_state_changes_until = datetime.now() + timedelta(seconds=self.grace_period)
            self.log.debug("call_service :: grace_period active, ignoring state changes until %s", self.ignore_state_changes_until)

        domain, e = entity.split(".")
        if service in ['turn_on','turn_off'] and domain in self.homeassistant_turn_on_domains:
            domain = "homeassistant"
            self.log.debug("call_service :: Actually calling service %s on %s via the %s domain because the entity domain requires it." % (service, entity, domain))
        params = {}
        if service_data is not None:
            params = service_data

        params["entity_id"] = entity
        asyncio.run_coroutine_threadsafe(
            self.hass.services.async_call(domain, service, service_data, context=self.context),
            self.hass.loop
        )
        self.update(service_data=service_data)

    def set_context(self, parent: Optional[Context] = None) -> None:
        """Set the context used when calling other services.

        The new ID is linked to the context (`parent`) of the triggering event
        and will be unique per trigger.
        """
        # Unique name per EC instance, but short enough to fit within id length
        name_hash = hashlib.sha1(self.name.encode("UTF-8")).hexdigest()[:6]
        self.log.debug("set_context :: name_hash: %s", name_hash)
        unique_id = uuid_util.random_uuid_hex()

        # Restrict id length to database field size
        context_id = f"{DOMAIN_SHORT}_{name_hash}_{unique_id}"[:CONTEXT_ID_CHARACTER_LIMIT]
        self.log.debug("set_context :: context_id: %s", context_id)
        self.context_id = context_id
        # parent_id only exists for a non-None parent
        parent_id = parent.id if parent else None
        self.context = Context(parent_id=parent_id, id=context_id)
        # Set the EC entity's context so the logbook can identify the source of
        # events that will be generated by this object.
        self.entity.async_set_context(self.context)

    def is_ignored_context(self, context: Context) -> bool:
        """Should the event with the given `context` be ignored?"""
        if any(re.match(pattern + r"\b", context.id) is not None
               for pattern in self.ignored_event_sources):
            # Matched an ignore_event_source regex pattern
            return True
        if context.id.startswith(f"{DOMAIN_SHORT}_"):
            # This is an EC-generated event
            return True
        return False

    def matches(self, value, list):
        """
            Checks whether a string is contained in a list (used for matching state strings)
        """
        try:
            index = list.index(value)
            return True
        except ValueError:
            return False

    def five_seconds_from_now(self, sun):
        """ Returns a timedelta that will result in a sunrise trigger in 5 seconds time"""

        return (
            dt.now()
            + timedelta(seconds=5)
            - get_astral_event_date(self.hass, sun, datetime.now())
        )

    def five_minutes_ago(self, sun):
        """ Returns a timedelta that will result in a sunrise trigger in 5 seconds time"""
        return (
            dt.now()
            - timedelta(minutes=5)
            - get_astral_event_date(self.hass, sun, datetime.now())
        )

    def add(self, list, config, key=None):
        """ Adds e (which can be a string or list or config or Template) to the list
            if e is defined.
            If its a template, we have to create the Template object and register state listeners
            self.add(self.controlEntities, config, CONF_CONTROL_ENTITIES)

        """
        self.log.debug("add :: Adding config key `%s` to the config list", key)
        if config is None:
            self.log.debug("Tried to configure %s but supplied config was None" % (key))
            return False

        v = None
        if key is not None:
            if key in config:  # must be in separate if statement
                v = config[key]
        else:
            v = config

        if isinstance(v,str):
            self.log.debug("Found string value %s for key %s, now adding to existing list %s. (Type: %s)", v, key, list, type(v))
            list.append(v)
            return len(v) > 0
        elif isinstance(v, List):
            self.log.debug("Found list value %s for key %s, now adding to existing list %s. (Type: %s)", v, key, list, type(v))
            list.extend(v)
            return len(v) > 0
        elif v == None:
            self.log.debug(f'Config key {key} not provided by user. Skipping.')
            return False
        else:
            self.log.error(f'Cannot determine type of provided config value. Key: {key}, Type: {type(v)}, Value: {str(v)}')
            return False

    def futurize(self, timet):
        """ Returns tomorrows time if time is in the past.
            Input time should be offset aware
         """

        # self.log.debug("-------------------- futurize ------------------------")
        # self.log.debug("Input (naive) %s ", timet)
        # Compare in HA-local wall time, NOT the process timezone: parse_time()
        # returns naive times in the HA-configured timezone, while
        # datetime.now()/date.today() follow the OS timezone. When the two
        # differ (e.g. host on UTC, HA on Europe/Prague), a just-fired callback
        # rescheduled itself to a time that is "future" in OS terms but already
        # past in HA-local terms — async_track_point_in_time then fires it
        # again immediately, in an endless loop (2026-07-09 incident: sunrise
        # end_time callbacks re-fired for exactly the UTC-offset window,
        # flooding MQTT with turn_off commands).
        now_local = dt.as_local(dt.now()).replace(tzinfo=None)
        today = now_local.date()
        try:
            t = datetime.combine(today, timet)
        except TypeError as e:
            t = timet
        if t.tzinfo is not None:
            t = dt.as_local(t).replace(tzinfo=None)
        x = now_local
        # self.log.debug("input time: " + str(t))

        # self.log.debug("current time: " + str(x))
        while t <= x:
            if t <= x:
                if self.debug_day_length is not None:
                    t = t + timedelta(seconds=int(self.debug_day_length))  # tomorrow!
                else:
                    t = t + timedelta(1)  # tomorrow!
                # self.log.debug( "Time already happened. Returning tomorrow instead. " + str(t))
            else:
                self.log.debug("Time still happening today. " + str(t))
        # self.log.debug("output time: %s", t)
        # self.log.debug("-------------------- futurize (END) -------------------")
        return t

    def debug_time_wrapper(self, timet):
        """

            Injects some debugging capability. Number is parenthesis is the
            first delay used on initial component setup. (This creates a time
            difference between start and end time callbacks.)

            The other number after the + sign is the standard period.
            In real life, this would be 24 hours, for debugging you can make
            it a few seconds to see the app change from idle to constrained.

            start_time: now + 5 (3)
            end_time: now + 5 (6)

            This function is used to wrap CONF_START_TIME and CONF_END_TIME
            and should only be called by the corresponding class properties!

            See config_times.
        """
        s = timet
        parts = re.search(r"^now\s*([+-])\s*(\d+)\s*\(?(\d+)?\)?$", timet)
        if parts:
            sign = parts.group(1)
            first_delay = parts.group(3)
            delay = parts.group(2)
            now = dt.now()
            self.log.debug("now %s", now)
            delta = timedelta(seconds=int(delay))
            if first_delay is not None:
                delta = timedelta(seconds=int(first_delay))
            if sign == "-":
                now = now - delta
            else:
                now = now + delta

            # self.log.debug("now + delta %s", now)

            s = str(self.make_naive(now).time().replace(microsecond=0))

        # self.log.debug("config time s %s", s)
        return s

    def log_config(self):
        self.log.debug("--------------------------------------------------")
        self.log.debug("       C O N F I G U R A T I O N   D U M P        ")
        self.log.debug("--------------------------------------------------")
        self.log.debug("Entity Controller       %s", self.name)
        self.log.debug("Sensor Entities         %s", str(self.sensorEntities))
        self.log.debug("Forced Sensor Entities: %s", str(self.forcedSensorEntities))
        self.log.debug("Event Bus Sensors:      %s", str(self.eventSensorTypes))
        self.log.debug("Control Entities:       %s", str(self.controlEntities))
        self.log.debug("State Entities:         %s", str(self.stateEntities))
        self.log.debug("Activate Trigger E.:    %s", str(self.triggerOnActivate))
        self.log.debug("Deactivate Trigger E.:  %s", str(self.triggerOnDeactivate))
        self.log.debug("Ignored state attrs:    %s", str(self.state_attributes_ignore))
        self.log.debug("Lux Entity:             %s", str(self.luxEntity))
        self.log.debug("Lux Threshold:          %s", str(self.lux_threshold))
        self.log.debug("Lux Bright States:      %s", str(self.lux_bright_states))
        self.log.debug("Night Mode Entity:      %s", str(self.night_mode_entity))
        self.log.debug("Night Mode States:      %s", str(self.night_mode_entity_states))
        self.log.debug("Night Control Entities: %s", str(self.nightControlEntities))
        self.log.debug("Light params:           %s", str(self.lightParams))
        self.log.debug("        -------        Time        -------        ")
        self.log.debug("Start time:             %s", self._start_time_private)
        self.log.debug("End time:               %s", self._end_time_private)
        self.log.debug("DT Now:                 %s", dt.now())
        self.log.debug("datetime Now:           %s", datetime.now())
        self.log.debug("Next Sunrise:           %s", self.next_sunrise(True))
        self.log.debug("Next Sunset:            %s", self.next_sunset(True))
        self.log.debug("        -------        Sun         -------        ")
        self.log.debug("Sunrise:                %s", self.sunrise(True))
        self.log.debug("Sunset:                 %s", self.sunset(True))
        self.log.debug("Sunset Diff (to now): %s", self.next_sunset() - dt.now())
        self.log.debug("Sunrise Diff(to now): %s", self.next_sunset() - dt.now())
        self.log.debug("Transition Behaviours: %s",  str(self.transition_behaviours))
        self.log.debug("--------------------------------------------------")

    def store_transition_behaviour(self, key, behaviour):
        """ manages transition_behaviour map """
        self.transition_behaviours[key] = behaviour

    def get_transition_behaviour(self, key):
        """ manages transition_behaviour map """
        if key in self.transition_behaviours:
            return self.transition_behaviours[key]
        else:
            return None

    def do_transition_behaviour(self, behaviour):
        """ Wrapper method for acting on transition behaviours such as at time of end constraint of state transitions from override state. """
        self.log.debug("%10s | Performing Transition Behaviour" % (behaviour))
        action = self.get_transition_behaviour(behaviour)
        if action:
            self.log.debug("%10s | Action - %s" % (behaviour, action))
            if action == CONF_TRANSITION_BEHAVIOUR_ON:
                self.log.debug("%10s | Performing Action - Turning on" % (behaviour))
                self.turn_on_control_entities()
            if action == CONF_TRANSITION_BEHAVIOUR_OFF:
                self.log.debug("%10s | Performing Action - Turning off" % (behaviour))
                self.turn_off_control_entities()
