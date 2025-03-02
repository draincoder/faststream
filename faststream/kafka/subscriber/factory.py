<<<<<<< HEAD
import warnings
from collections.abc import Collection, Iterable
from typing import TYPE_CHECKING, Optional, Union
=======
from typing import (
    TYPE_CHECKING,
    Collection,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b

from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
<<<<<<< HEAD
from faststream.kafka.subscriber.specified import (
    SpecificationBatchSubscriber,
    SpecificationConcurrentBetweenPartitionsSubscriber,
    SpecificationConcurrentDefaultSubscriber,
    SpecificationDefaultSubscriber,
=======
from faststream.kafka.subscriber.asyncapi import (
    AsyncAPIBatchSubscriber,
    AsyncAPIConcurrentBetweenPartitionsSubscriber,
    AsyncAPIConcurrentDefaultSubscriber,
    AsyncAPIDefaultSubscriber,
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b
)
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord, TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener
    from fast_depends.dependencies import Dependant

<<<<<<< HEAD
    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import BrokerMiddleware
=======
    from faststream.broker.types import BrokerMiddleware
    from faststream.types import AnyDict


@overload
def create_subscriber(
    *topics: str,
    batch: Literal[True],
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence["BrokerMiddleware[Tuple[ConsumerRecord, ...]]"],
    # AsyncAPI args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> "AsyncAPIBatchSubscriber": ...


@overload
def create_subscriber(
    *topics: str,
    batch: Literal[False],
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
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
    batch: bool,
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
    is_manual: bool,
    # Subscriber args
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    retry: bool,
    broker_dependencies: Iterable["Depends"],
    broker_middlewares: Sequence[
        "BrokerMiddleware[Union[ConsumerRecord, Tuple[ConsumerRecord, ...]]]"
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
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b


def create_subscriber(
    *topics: str,
    batch: bool,
    batch_timeout_ms: int,
    max_records: Optional[int],
    # Kafka information
    group_id: Optional[str],
    listener: Optional["ConsumerRebalanceListener"],
    pattern: Optional[str],
    connection_args: "AnyDict",
    partitions: Collection["TopicPartition"],
<<<<<<< HEAD
    auto_commit: bool,
=======
    is_manual: bool,
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b
    # Subscriber args
    ack_policy: "AckPolicy",
    max_workers: int,
    no_ack: bool,
    no_reply: bool,
    broker_dependencies: Collection["Dependant"],
    broker_middlewares: Collection[
        "BrokerMiddleware[Union[ConsumerRecord, tuple[ConsumerRecord, ...]]]"
    ],
    # Specification args
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
<<<<<<< HEAD
    "SpecificationDefaultSubscriber",
    "SpecificationBatchSubscriber",
    "SpecificationConcurrentDefaultSubscriber",
    "SpecificationConcurrentBetweenPartitionsSubscriber",
=======
    "AsyncAPIDefaultSubscriber",
    "AsyncAPIBatchSubscriber",
    "AsyncAPIConcurrentDefaultSubscriber",
    "AsyncAPIConcurrentBetweenPartitionsSubscriber",
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b
]:
    _validate_input_for_misconfigure(
        *topics,
        pattern=pattern,
        partitions=partitions,
        ack_policy=ack_policy,
        no_ack=no_ack,
        auto_commit=auto_commit,
        max_workers=max_workers,
        group_id=group_id,
    )

<<<<<<< HEAD
    if auto_commit is not EMPTY:
        ack_policy = AckPolicy.ACK_FIRST if auto_commit else AckPolicy.REJECT_ON_ERROR
=======
    if is_manual and max_workers > 1:
        if len(topics) > 1:
            raise SetupError(
                "You must use a single topic with concurrent manual commit mode."
            )
        if pattern is not None:
            raise SetupError(
                "You can not use a pattern with concurrent manual commit mode."
            )
        if partitions:
            raise SetupError(
                "Manual partition assignment is not supported with concurrent manual commit mode."
            )
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b

    if no_ack is not EMPTY:
        ack_policy = AckPolicy.DO_NOTHING if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.ACK_FIRST

    if ack_policy is AckPolicy.ACK_FIRST:
        connection_args["enable_auto_commit"] = True
        ack_policy = AckPolicy.DO_NOTHING
        ack_first = True
    else:
        ack_first = False

    if batch:
        return SpecificationBatchSubscriber(
            *topics,
            batch_timeout_ms=batch_timeout_ms,
            max_records=max_records,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

<<<<<<< HEAD
    if max_workers > 1:
        if ack_first:
            return SpecificationConcurrentDefaultSubscriber(
                *topics,
                max_workers=max_workers,
=======
    else:
        if max_workers > 1:
            if is_manual:
                return AsyncAPIConcurrentBetweenPartitionsSubscriber(
                    topics[0],
                    max_workers=max_workers,
                    group_id=group_id,
                    listener=listener,
                    pattern=pattern,
                    connection_args=connection_args,
                    partitions=partitions,
                    is_manual=is_manual,
                    no_ack=no_ack,
                    no_reply=no_reply,
                    retry=retry,
                    broker_dependencies=broker_dependencies,
                    broker_middlewares=broker_middlewares,
                    title_=title_,
                    description_=description_,
                    include_in_schema=include_in_schema,
                )
            else:
                return AsyncAPIConcurrentDefaultSubscriber(
                    *topics,
                    max_workers=max_workers,
                    group_id=group_id,
                    listener=listener,
                    pattern=pattern,
                    connection_args=connection_args,
                    partitions=partitions,
                    is_manual=is_manual,
                    no_ack=no_ack,
                    no_reply=no_reply,
                    retry=retry,
                    broker_dependencies=broker_dependencies,
                    broker_middlewares=broker_middlewares,
                    title_=title_,
                    description_=description_,
                    include_in_schema=include_in_schema,
                )
        else:
            return AsyncAPIDefaultSubscriber(
                *topics,
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b
                group_id=group_id,
                listener=listener,
                pattern=pattern,
                connection_args=connection_args,
                partitions=partitions,
<<<<<<< HEAD
                ack_policy=ack_policy,
                no_reply=no_reply,
=======
                is_manual=is_manual,
                no_ack=no_ack,
                no_reply=no_reply,
                retry=retry,
>>>>>>> 60c04eb6d5ecdeef8d958c197adaf2ffef193e2b
                broker_dependencies=broker_dependencies,
                broker_middlewares=broker_middlewares,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
            )

        return SpecificationConcurrentBetweenPartitionsSubscriber(
            topics[0],
            max_workers=max_workers,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_dependencies=broker_dependencies,
            broker_middlewares=broker_middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    return SpecificationDefaultSubscriber(
        *topics,
        group_id=group_id,
        listener=listener,
        pattern=pattern,
        connection_args=connection_args,
        partitions=partitions,
        ack_policy=ack_policy,
        no_reply=no_reply,
        broker_dependencies=broker_dependencies,
        broker_middlewares=broker_middlewares,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )


def _validate_input_for_misconfigure(
    *topics: str,
    partitions: Iterable["TopicPartition"],
    pattern: Optional[str],
    ack_policy: "AckPolicy",
    auto_commit: bool,
    no_ack: bool,
    group_id: Optional[str],
    max_workers: int,
) -> None:
    if auto_commit is not EMPTY:
        warnings.warn(
            "`auto_commit` option was deprecated in prior to `ack_policy=AckPolicy.ACK_FIRST`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `auto_commit` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.ACK_FIRST if auto_commit else AckPolicy.REJECT_ON_ERROR

    if no_ack is not EMPTY:
        warnings.warn(
            "`no_ack` option was deprecated in prior to `ack_policy=AckPolicy.DO_NOTHING`. Scheduled to remove in 0.7.0",
            category=DeprecationWarning,
            stacklevel=4,
        )

        if ack_policy is not EMPTY:
            msg = "You can't use deprecated `no_ack` and `ack_policy` simultaneously. Please, use `ack_policy` only."
            raise SetupError(msg)

        ack_policy = AckPolicy.DO_NOTHING if no_ack else EMPTY

    if ack_policy is EMPTY:
        ack_policy = AckPolicy.ACK_FIRST

    if max_workers > 1 and ack_policy is not AckPolicy.ACK_FIRST:
        if len(topics) > 1:
            msg = "You must use a single topic with concurrent manual commit mode."
            raise SetupError(msg)

        if pattern is not None:
            msg = "You can not use a pattern with concurrent manual commit mode."
            raise SetupError(msg)

        if partitions:
            msg = "Manual partition assignment is not supported with concurrent manual commit mode."
            raise SetupError(msg)

    if not topics and not partitions and not pattern:
        msg = "You should provide either `topics` or `partitions` or `pattern`."
        raise SetupError(msg)

    if topics and partitions:
        msg = "You can't provide both `topics` and `partitions`."
        raise SetupError(msg)

    if topics and pattern:
        msg = "You can't provide both `topics` and `pattern`."
        raise SetupError(msg)

    if partitions and pattern:
        msg = "You can't provide both `partitions` and `pattern`."
        raise SetupError(msg)
