"""Event schemas (pydantic v2), topic routing and the shared envelope.

Every event = envelope (event_id, event_type, schema_version, event_ts, emitted_ts, source)
+ a type-specific `data` payload. `event_ts` is when it happened; `emitted_ts` is when the
producer sent it. The gap between the two is what makes an event "late".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from simulator.clock import iso

SCHEMA_VERSION = 1
TOPIC_ORDERS = "fooddel.orders.v1"
TOPIC_DRIVER_LOCATIONS = "fooddel.driver_locations.v1"
TOPIC_REFERENCE = "fooddel.reference.v1"
TOPICS = (TOPIC_ORDERS, TOPIC_DRIVER_LOCATIONS, TOPIC_REFERENCE)

Source = Literal["customer_app", "restaurant_app", "driver_app", "platform"]


class _Data(BaseModel):
    model_config = ConfigDict(extra="forbid")


class OrderItem(_Data):
    item_id: str
    name: str
    qty: int = Field(ge=1)
    unit_price_inr: int = Field(ge=0)


class OrderPlaced(_Data):
    order_id: str
    customer_id: str
    restaurant_id: str
    zone_id: str
    h3_r8: str
    delivery_lat: float
    delivery_lng: float
    items: list[OrderItem] = Field(min_length=1)
    subtotal_inr: int = Field(ge=0)
    delivery_fee_inr: int = Field(ge=0)
    surge_fee_inr: int = Field(ge=0)
    discount_inr: int = Field(ge=0)
    total_inr: int = Field(ge=0)
    payment_method: Literal["UPI", "CARD", "COD", "WALLET"]
    promised_eta_min: int = Field(gt=0)
    is_raining: bool


class OrderAccepted(_Data):
    order_id: str
    restaurant_id: str
    estimated_prep_min: int = Field(gt=0)


class DriverAssigned(_Data):
    order_id: str
    driver_id: str
    assign_attempts: int = Field(ge=1)
    driver_distance_km: float = Field(ge=0)
    search_ring_k: int = Field(ge=0)


class FoodReady(_Data):
    order_id: str
    restaurant_id: str
    actual_prep_min: float = Field(gt=0)


class OrderPickedUp(_Data):
    order_id: str
    driver_id: str
    lat: float
    lng: float


class OrderDelivered(_Data):
    order_id: str
    driver_id: str
    lat: float
    lng: float
    distance_km: float = Field(ge=0)
    actual_delivery_min: float = Field(gt=0)


class OrderCancelled(_Data):
    order_id: str
    cancelled_by: Literal["CUSTOMER", "RESTAURANT", "PLATFORM"]
    reason: Literal[
        "RESTAURANT_REJECTED", "CUSTOMER_CHANGED_MIND", "NO_DRIVER_AVAILABLE", "ITEM_UNAVAILABLE"
    ]
    state_at_cancel: str


class DriverLocation(_Data):
    driver_id: str
    lat: float
    lng: float
    h3_r8: str
    zone_id: str | None  # None when the driver is between zones
    status: Literal["IDLE", "ASSIGNED", "AT_RESTAURANT", "DELIVERING", "OFFLINE"]
    order_id: str | None
    speed_kmh: float = Field(ge=0)
    battery_pct: int = Field(ge=0, le=100)


class RestaurantUpdated(_Data):
    restaurant_id: str
    name: str
    zone_id: str
    h3_r8: str
    lat: float
    lng: float
    cuisine: str
    price_band: int = Field(ge=1, le=3)
    avg_prep_min: float = Field(gt=0)
    rating: float = Field(ge=0, le=5)
    is_active: bool
    change_type: Literal["SNAPSHOT", "UPDATE"]
    changed_fields: list[str]


class CustomerRegistered(_Data):
    customer_id: str
    full_name: str
    email: str
    phone: str
    zone_id: str
    h3_r8: str
    home_lat: float
    home_lng: float
    signup_channel: Literal["ORGANIC", "REFERRAL", "PAID_AD"]


class WeatherUpdated(_Data):
    city: str
    condition: Literal["CLEAR", "RAIN"]
    intensity: Literal["LIGHT", "MODERATE", "HEAVY"] | None
    demand_multiplier: float
    supply_multiplier: float
    travel_time_multiplier: float


@dataclass(frozen=True)
class EventSpec:
    model: type[_Data]
    topic: str
    key_field: str  # Kafka message key -> same key, same partition, ordered
    source: str  # default source app


EVENT_SPECS: dict[str, EventSpec] = {
    "ORDER_PLACED": EventSpec(OrderPlaced, TOPIC_ORDERS, "order_id", "customer_app"),
    "ORDER_ACCEPTED": EventSpec(OrderAccepted, TOPIC_ORDERS, "order_id", "restaurant_app"),
    "DRIVER_ASSIGNED": EventSpec(DriverAssigned, TOPIC_ORDERS, "order_id", "platform"),
    "FOOD_READY": EventSpec(FoodReady, TOPIC_ORDERS, "order_id", "restaurant_app"),
    "ORDER_PICKED_UP": EventSpec(OrderPickedUp, TOPIC_ORDERS, "order_id", "driver_app"),
    "ORDER_DELIVERED": EventSpec(OrderDelivered, TOPIC_ORDERS, "order_id", "driver_app"),
    "ORDER_CANCELLED": EventSpec(OrderCancelled, TOPIC_ORDERS, "order_id", "platform"),
    "DRIVER_LOCATION": EventSpec(DriverLocation, TOPIC_DRIVER_LOCATIONS, "driver_id", "driver_app"),
    "RESTAURANT_UPDATED": EventSpec(
        RestaurantUpdated, TOPIC_REFERENCE, "restaurant_id", "platform"
    ),
    "CUSTOMER_REGISTERED": EventSpec(
        CustomerRegistered, TOPIC_REFERENCE, "customer_id", "customer_app"
    ),
    "WEATHER_UPDATED": EventSpec(WeatherUpdated, TOPIC_REFERENCE, "city", "platform"),
}
EventType = Literal[tuple(EVENT_SPECS)]  # type: ignore[valid-type]


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_type: EventType
    schema_version: int
    event_ts: str
    emitted_ts: str | None
    source: Source
    data: dict


@dataclass
class OutEvent:
    """An event on its way to a sink: routing info + the JSON value."""

    topic: str
    key: str
    value: dict
    event_ts: float  # numeric copy of value["event_ts"], avoids re-parsing


def build_event(
    event_type: str, event_id: str, ts: float, data: dict, source: str | None = None
) -> OutEvent:
    spec = EVENT_SPECS[event_type]
    payload = spec.model(**data).model_dump(mode="json")  # validates on construction
    value = {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "event_ts": iso(ts),
        "emitted_ts": None,
        "source": source or spec.source,
        "data": payload,
    }
    return OutEvent(spec.topic, str(payload[spec.key_field]), value, ts)


def validate_event(value: dict) -> None:
    """Raise pydantic.ValidationError if the envelope or payload is invalid."""
    env = Envelope.model_validate(value)
    EVENT_SPECS[env.event_type].model.model_validate(env.data)


def json_schemas() -> dict[str, dict]:
    """JSON Schema per event type (envelope with a typed `data`), for the P2 schema registry."""
    out = {}
    for event_type, spec in EVENT_SPECS.items():
        schema = Envelope.model_json_schema()
        data_schema = spec.model.model_json_schema()
        schema["title"] = event_type
        schema["properties"]["event_type"] = {"const": event_type}
        schema["properties"]["data"] = {k: v for k, v in data_schema.items() if k != "$defs"}
        if "$defs" in data_schema:
            schema["$defs"] = data_schema["$defs"]
        out[event_type] = schema
    return out
