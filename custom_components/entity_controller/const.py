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
""" Constants used by other files """

DOMAIN = "entity_controller"
DOMAIN_SHORT = "ec"

# services
SERVICE_ACTIVATE = "activate"
SERVICE_CLEAR_BLOCK = "clear_block"
SERVICE_ENABLE_BLOCK = "enable_block"
SERVICE_ENABLE_STAY_MODE = "enable_stay_mode"
SERVICE_DISABLE_STAY_MODE = "disable_stay_mode"
SERVICE_SET_NIGHT_MODE = "set_night_mode"

# Forced sensors (bypass blocked/constrained/overridden states)
CONF_FORCED_SENSORS = "forced_sensors"
# Sensors that NEVER switch the light on, but keep it on (keep-alive).
# Use case: trigger from a PIR (fast, reliable edge), hold from PIR + radar
# (a radar also sees a motionless person). A radar listed in sensors: would
# switch the light on while flapping too — the 103 radar toggles 54x/day,
# with 16 % of its gaps <= 60 s.
CONF_HOLD_SENSORS = "hold_sensors"
# Safety cap on how long a hold sensor may keep the timer alive. A hold sensor
# that reports "occupied" forever (wedged mmWave radar) otherwise means the
# light NEVER turns off — the timer never even starts, EC just reports
# expires_at: "pending sensor".
# Measured 2026-08-28: 203-presence (SNZB-06P) sat at occupancy=true for ~42 h
# while still answering config writes and a z2m reconfigure; light.203_b was on
# for 2 h 18 min until a human switched it off. Only a power-cycle cleared it.
# Applies to hold sensors ONLY: a stuck sensor in `sensors:` would re-trigger
# immediately, so ignoring it there would just make the light flicker.
CONF_HOLD_MAX_SECONDS = "hold_max_seconds"

# Event bus sensor support
CONF_EVENT_SENSORS = "event_sensors"
CONF_EVENT_SENSOR_TYPE = "ha_event"  # sensor_type value for HA bus events

# Illuminance (lux) activation constraint
CONF_LUX_ENTITY = "lux_entity"
CONF_LUX_THRESHOLD = "lux_threshold"
CONF_LUX_BRIGHT_STATES = "lux_bright_states"

# State persistence
STORAGE_VERSION = 1
STORAGE_KEY_PREFIX = "entity_controller_state_"

#configuration
CONF_START_TIME = 'start_time'
CONF_END_TIME = 'end_time'

# Transition Behaviours
CONF_BEHAVIOURS = 'behaviours'

CONF_ON_ENTER_IDLE='on_enter_idle'
CONF_ON_EXIT_IDLE='on_exit_idle'
CONF_ON_ENTER_ACTIVE='on_enter_active'
CONF_ON_EXIT_ACTIVE='on_exit_active'
CONF_ON_ENTER_OVERRIDDEN = 'on_enter_overridden'
CONF_ON_EXIT_OVERRIDDEN = 'on_exit_overridden'
CONF_ON_ENTER_CONSTRAINED = 'on_enter_constrained'
CONF_ON_EXIT_CONSTRAINED = 'on_exit_constrained'
CONF_ON_ENTER_BLOCKED = 'on_enter_blocked'
CONF_ON_EXIT_BLOCKED = 'on_exit_blocked'

CONF_TRANSITION_BEHAVIOUR_ON = 'on'
CONF_TRANSITION_BEHAVIOUR_OFF = 'off'
CONF_TRANSITION_BEHAVIOUR_IGNORE = 'ignore'
SENSOR_TYPE_DURATION = "duration"
SENSOR_TYPE_EVENT = "event"
MODE_DAY = "day"
MODE_NIGHT = "night"

DEFAULT_DELAY = 180
DEFAULT_BRIGHTNESS = 100
DEFAULT_NAME = "Entity Timer"

# CONF_NAME = 'slug'
CONF_CONTROL_ENTITIES = "entities"
CONF_CONTROL_ENTITY = "entity"
CONF_TRIGGER_ON_ACTIVATE = "trigger_on_activate"
CONF_TRIGGER_ON_DEACTIVATE = "trigger_on_deactivate"
CONF_SENSOR = "sensor"
CONF_SENSORS = "sensors"
CONF_SERVICE_DATA = "service_data"
CONF_SERVICE_DATA_OFF = "service_data_off"
CONF_STATE_ENTITIES = "state_entities"
CONF_DELAY = "delay"
CONF_BLOCK_TIMEOUT = "block_timeout"
CONF_DISABLE_BLOCK = "disable_block"
CONF_SENSOR_TYPE_DURATION = "sensor_type_duration"
CONF_SENSOR_TYPE = "sensor_type"
CONF_SENSOR_RESETS_TIMER = "sensor_resets_timer"
CONF_NIGHT_MODE = "night_mode"
# night_mode sub-keys: state-entity-driven night detection + alternate targets
CONF_NIGHT_MODE_ENTITY = "entity"
CONF_NIGHT_MODE_ENTITY_STATES = "entity_states"
CONF_NIGHT_MODE_ENTITIES = "entities"
CONF_STATE_ATTRIBUTES_IGNORE = "state_attributes_ignore"
CONF_IGNORED_EVENT_SOURCES = "ignored_event_sources"
MODE_DAY = 'day'
MODE_NIGHT = 'night'
CONSTRAIN_START = 1
CONSTRAIN_END = 2
STATES = ['pending', 'idle', 'overridden', 'constrained', 'blocked',
          {'name': 'active', 'children': ['timer', 'stay_on'],
           'initial': False}]
CONF_IGNORE_STATE_CHANGES_UNTIL = "grace_period"


CONTEXT_ID_CHARACTER_LIMIT = 26
