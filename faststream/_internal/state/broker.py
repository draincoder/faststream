from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:

    from .fast_depends import DIState
    from .logger import LoggerState


class BrokerState(Protocol):
    di_state: "DIState"
    logger_state: "LoggerState"

    def _setup(self) -> None: ...

    def _setup_logger_state(self) -> None: ...

    def __bool__(self) -> bool: ...


class _EmptyBrokerState(BrokerState):
    def __init__(self, error_msg: str) -> None:
        self.error_msg = error_msg

    @property
    def logger_state(self) -> "LoggerState":
        raise IncorrectState(self.error_msg)

    @logger_state.setter
    def logger_state(self, value: "LoggerState", /) -> None:
        raise IncorrectState(self.error_msg)

    def _setup(self) -> None:
        pass

    def _setup_logger_state(self) -> None:
        pass

    def __bool__(self) -> bool:
        return False


class EmptyBrokerState(_EmptyBrokerState):
    @property
    def di_state(self) -> "DIState":
        raise IncorrectState(self.error_msg)

    @di_state.setter
    def di_state(self, value: "DIState", /) -> None:
        raise IncorrectState(self.error_msg)


class OuterBrokerState(_EmptyBrokerState):
    def __init__(self, *, di_state: "DIState") -> None:
        self.di_state = di_state

    def __bool__(self) -> bool:
        return True


class InitialBrokerState(BrokerState):
    def __init__(
        self,
        *,
        di_state: "DIState",
        logger_state: "LoggerState",
    ) -> None:
        self.di_state = di_state
        self.logger_state = logger_state

        self.setupped = False

    def _setup(self) -> None:
        self.setupped = True

    def _setup_logger_state(self) -> None:
        self.logger_state._setup(context=self.di_state.context)

    def __bool__(self) -> bool:
        return self.setupped
