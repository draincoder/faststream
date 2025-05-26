from abc import abstractmethod
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
)

from faststream._internal.endpoint.publisher import PublisherProto
from faststream._internal.endpoint.specification.base import SpecificationEndpoint
from faststream._internal.endpoint.subscriber import (
    SubscriberProto,
)
from faststream._internal.state import BrokerState, Pointer
from faststream._internal.types import BrokerMiddleware, MsgType
from faststream.specification.schema import PublisherSpec, SubscriberSpec

from .config import BrokerConfig

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant


class FinalSubscriber(
    SpecificationEndpoint[MsgType, SubscriberSpec],
    SubscriberProto[MsgType],
):
    @property
    @abstractmethod
    def call_name(self) -> str:
        raise NotImplementedError


class FinalPublisher(
    SpecificationEndpoint[MsgType, PublisherSpec],
    PublisherProto[MsgType],
):
    pass


class ABCBroker(Generic[MsgType]):
    """Basic class for brokers and routers.

    Contains subscribers & publishers registration logic only.
    """

    _subscribers: list[FinalSubscriber[MsgType]]
    _publishers: list[FinalPublisher[MsgType]]

    def __init__(
        self,
        *,
        config: "BrokerConfig",
        state: "BrokerState",
        routers: Sequence["ABCBroker[MsgType]"],
    ) -> None:
        self.config = config
        self._parser = config.broker_parser
        self._decoder = config.broker_decoder

        self._subscribers = []
        self._publishers = []

        self._state = Pointer(state)

        self.include_routers(*routers)

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        """Append BrokerMiddleware to the end of middlewares list.

        Current middleware will be used as a most inner of already existed ones.
        """
        self.config.add_middleware(middleware)

    @abstractmethod
    def subscriber(
        self,
        subscriber: "FinalSubscriber[MsgType]",
    ) -> "FinalSubscriber[MsgType]":
        self._subscribers.append(subscriber)
        return subscriber

    @abstractmethod
    def publisher(
        self,
        publisher: "FinalPublisher[MsgType]",
    ) -> "FinalPublisher[MsgType]":
        self._publishers.append(publisher)
        self.setup_publisher(publisher)
        return publisher

    def setup_publisher(
        self,
        publisher: "FinalPublisher[MsgType]",
        **kwargs: Any,
    ) -> None:
        """Setup the Publisher to prepare it to starting."""
        publisher._setup(**kwargs, state=self._state)

    def _setup(self, state: Optional["BrokerState"]) -> None:
        if state is not None:
            self._state.set(state)

    def include_router(
        self,
        router: "ABCBroker[Any]",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Iterable["BrokerMiddleware[MsgType]"] = (),
        include_in_schema: Optional[bool] = None,
    ) -> None:
        """Includes a router in the current object."""
        router._setup(self._state.get())

        new_config = self.config | BrokerConfig(
            prefix=prefix,
            include_in_schema=include_in_schema,
            broker_middlewares=middlewares,
            broker_dependencies=dependencies,
        )

        for sub in router._subscribers:
            sub.register(new_config)
            self._subscribers.append(sub)

        for pub in router._publishers:
            pub.register(new_config)
            self._publishers.append(pub)

    def include_routers(
        self,
        *routers: "ABCBroker[MsgType]",
    ) -> None:
        """Includes routers in the object."""
        for r in routers:
            self.include_router(r)
