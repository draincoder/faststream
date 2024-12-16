from typing import (
    TYPE_CHECKING,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)

from faststream.confluent.subscriber.asyncapi import (
    AsyncAPIBatchSubscriber,
    AsyncAPIConcurrentDefaultSubscriber,
    AsyncAPIDefaultSubscriber,
)
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from confluent_kafka import Message as ConfluentMsg
    from fast_depends.dependencies import Depends

    from faststream.broker.types import BrokerMiddleware
    from faststream.confluent.schemas import TopicPartition
    from faststream.types import AnyDict


@overload
def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: Literal[True],
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    no_ack: bool,
    max_workers: int,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence["BrokerMiddleware[Tuple[ConfluentMsg, ...]]"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "AsyncAPIBatchSubscriber": ...


@overload
def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: Literal[False],
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    no_ack: bool,
    max_workers: int,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence["BrokerMiddleware[ConfluentMsg]"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
]: ...


@overload
def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: bool,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    no_ack: bool,
    max_workers: int,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Union[
        Sequence["BrokerMiddleware[Tuple[ConfluentMsg, ...]]"],
        Sequence["BrokerMiddleware[ConfluentMsg]"],
    ],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIBatchSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
]: ...


def create_subscriber(
    *topics: str,
    partitions: Sequence["TopicPartition"],
    polling_interval: float,
    batch: bool,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    connection_data: "AnyDict",
    is_manual: bool,
    # Subscriber args
    no_ack: bool,
    max_workers: int,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Union[
        Sequence["BrokerMiddleware[Tuple[ConfluentMsg, ...]]"],
        Sequence["BrokerMiddleware[ConfluentMsg]"],
    ],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIBatchSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
]:
    if is_manual and max_workers > 1:
        raise SetupError("Max workers not work with manual commit mode.")

    if batch:
        return AsyncAPIBatchSubscriber(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            max_records=max_records,
            group_id=group_id,
            connection_data=connection_data,
            is_manual=is_manual,
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_dependencies=broker_dependencies,
            broker_middlewares=cast(
                Sequence["BrokerMiddleware[Tuple[ConfluentMsg, ...]]"],
                broker_middlewares,
            ),
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
    else:
        if max_workers > 1:
            return AsyncAPIConcurrentDefaultSubscriber(
                *topics,
                max_workers=max_workers,
                partitions=partitions,
                polling_interval=polling_interval,
                group_id=group_id,
                connection_data=connection_data,
                is_manual=is_manual,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=cast(
                    Sequence["BrokerMiddleware[ConfluentMsg]"],
                    broker_middlewares,
                ),
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )
        else:
            return AsyncAPIDefaultSubscriber(
                *topics,
                partitions=partitions,
                polling_interval=polling_interval,
                group_id=group_id,
                connection_data=connection_data,
                is_manual=is_manual,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
                broker_dependencies=broker_dependencies,
                broker_middlewares=cast(
                    Sequence["BrokerMiddleware[ConfluentMsg]"],
                    broker_middlewares,
                ),
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )