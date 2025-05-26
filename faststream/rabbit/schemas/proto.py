from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from faststream.rabbit.configs.specification import RabbitSpecificationConfig

    from .exchange import RabbitExchange
    from .queue import RabbitQueue


class BaseRMQInformation:
    """Base class to store Specification RMQ bindings."""

    virtual_host: str
    queue: "RabbitQueue"
    exchange: "RabbitExchange"
    app_id: Optional[str]

    def __init__(self, config: "RabbitSpecificationConfig", /, **kwargs: Any) -> None:
        super().__init__(config, **kwargs)

        self.queue = config.queue
        self.exchange = config.exchange

        # Setup it later
        self.app_id = config.config.app_id
        self.virtual_host = config.config.virtual_host
