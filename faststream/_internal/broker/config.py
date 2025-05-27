from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.logger import LoggerState
from faststream._internal.producer import ProducerProto, ProducerUnset
from faststream._internal.di import FastDependsConfig

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.broker import BrokerConfig
    from faststream._internal.types import AsyncCallable, BrokerMiddleware, MsgType


@dataclass(kw_only=True)
class BrokerConfig:
    prefix: str = ""
    include_in_schema: Optional[bool] = True

    broker_middlewares: Sequence["BrokerMiddleware[MsgType]"]
    broker_parser: Optional["AsyncCallable"] = None
    broker_decoder: Optional["AsyncCallable"] = None

    producer: "ProducerProto" = field(default_factory=ProducerUnset)
    logger: LoggerState = field(default_factory=LoggerState)
    fd_config: "FastDependsConfig" = field(default_factory=FastDependsConfig)

    # subscriber options
    broker_dependencies: Iterable["Dependant"]
    graceful_timeout: Optional[float] = None
    extra_context: "AnyDict" = field(default_factory=dict)

    def add_middleware(self, middleware: "BrokerMiddleware[MsgType]") -> None:
        self.broker_middlewares = (*self.broker_middlewares, middleware)

    def __or__(self, value: "BrokerConfig", /) -> "BrokerConfig":
        return BrokerConfig(**self._merge_configs(value))

    def _merge_configs(self, value: "BrokerConfig", /) -> "AnyDict":
        return {
            "prefix": f"{self.prefix}{value.prefix}",
            "include_in_schema": self._solve_include_in_schema(value),
            "broker_middlewares": (*self.broker_middlewares, *value.broker_middlewares),
            "broker_dependencies": (
                *self.broker_dependencies,
                *value.broker_dependencies,
            ),
            "broker_parser": self.broker_parser or value.broker_parser,
            "broker_decoder": self.broker_decoder or value.broker_decoder,
            "extra_context": {**value.extra_context, **self.extra_context},
            "graceful_timeout": self.graceful_timeout,
            "producer": self.producer,
            "logger": self.logger,
            "fd_config": self.fd_config,
        }

    def _solve_include_in_schema(self, value: "BrokerConfig", /) -> bool:
        """Resolve router `include_in_schema` option with current value respect.

        >>> include_in_schema = Any, self.include_in_schema = False
        False

        >>> include_in_schema = False, self.include_in_schema = True | None
        False

        >>> include_in_schema = True | None, self.include_in_schema = True | None
        True
        """
        outer_include = (
            True if value.include_in_schema is None else value.include_in_schema
        )

        if self.include_in_schema is None:
            return outer_include

        if not self.include_in_schema:
            return False

        return outer_include
