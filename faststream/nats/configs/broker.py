from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.broker import BrokerConfig
from faststream.nats.broker.state import BrokerState
from faststream.nats.helpers import KVBucketDeclarer, OSBucketDeclarer
from faststream.nats.publisher.producer import (
    FakeNatsFastProducer,
    NatsFastProducer,
    NatsJSFastProducer,
)

if TYPE_CHECKING:
    from nats.aio.client import Client


@dataclass(kw_only=True)
class NatsBrokerConfig(BrokerConfig):
    producer: "NatsFastProducer" = field(default_factory=FakeNatsFastProducer)
    js_producer: "NatsJSFastProducer" = field(default_factory=FakeNatsFastProducer)
    connection_state: BrokerState = field(default_factory=BrokerState)
    kv_declarer: KVBucketDeclarer = field(default_factory=KVBucketDeclarer)
    os_declarer: OSBucketDeclarer = field(default_factory=OSBucketDeclarer)

    def __or__(self, value: "BrokerConfig", /) -> "NatsBrokerConfig":
        return NatsBrokerConfig(
            os_declarer=self.os_declarer,
            kv_declarer=self.kv_declarer,
            connection_state=self.connection_state,
            js_producer=self.js_producer,
            **self._merge_configs(value),
        )

    def connect(self, connection: "Client") -> None:
        stream = connection.jetstream()

        self.producer.connect(connection)

        self.js_producer.connect(stream)
        self.kv_declarer.connect(stream)
        self.os_declarer.connect(stream)

        self.connection_state.connect(connection, stream)

    def disconnect(self) -> None:
        self.producer.disconnect()
        self.js_producer.disconnect()
        self.kv_declarer.disconnect()
        self.os_declarer.disconnect()

        self.connection_state.disconnect()
