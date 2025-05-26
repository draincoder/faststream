from typing import Any

import pytest

from faststream.rabbit import RabbitBroker, RabbitRouter


class IncludeTestcase:
    def get_object(self, router: RabbitRouter) -> Any:
        raise NotImplementedError

    def test_broker_middlewares(self) -> None:
        broker = RabbitBroker(middlewares=(1,))

        obj = self.get_object(broker)

        broker_middlewars = obj._outer_config.broker_middlewares
        assert broker_middlewars == (1,), broker_middlewars

    def test_router_middlewares(self) -> None:
        broker = RabbitBroker(middlewares=(1,))

        router = RabbitRouter(middlewares=[2])

        obj = self.get_object(router)

        broker.include_router(router)

        broker_middlewars = obj._outer_config.broker_middlewares
        assert broker_middlewars == (1, 2), broker_middlewars

    def test_nested_router_middleware(self) -> None:
        broker = RabbitBroker(middlewares=(1,))

        router = RabbitRouter(middlewares=[2])

        router2 = RabbitRouter(middlewares=[3])

        obj = self.get_object(router2)

        router.include_router(router2)
        broker.include_router(router)

        broker_middlewars = obj._outer_config.broker_middlewares
        assert broker_middlewars == (1, 2, 3), broker_middlewars

    def test_include_router_with_middlewares(self) -> None:
        broker = RabbitBroker(middlewares=(1,))

        router = RabbitRouter(middlewares=[3])

        obj = self.get_object(router)

        broker.include_router(router, middlewares=[2])

        broker_middlewars = obj._outer_config.broker_middlewares
        assert broker_middlewars == (1, 2, 3), broker_middlewars

    @pytest.mark.parametrize(
        ("router", "include", "result"),
        (
            pytest.param(
                RabbitRouter(include_in_schema=False), True, False, id="visible router"
            ),
            pytest.param(
                RabbitRouter(include_in_schema=True), True, True, id="invisible include"
            ),
            pytest.param(RabbitRouter(), True, True, id="default router"),
            pytest.param(
                RabbitRouter(include_in_schema=False),
                False,
                False,
                id="ignore visible router",
            ),
            pytest.param(
                RabbitRouter(include_in_schema=True),
                False,
                False,
                id="ignore invisible router",
            ),
            pytest.param(RabbitRouter(), False, False, id="ignore default router"),
        ),
    )
    def test_router_include_in_schema(
        self, router: RabbitRouter, include: bool, result: bool
    ) -> None:
        broker = RabbitBroker()

        obj = self.get_object(router)
        broker.include_router(router, include_in_schema=include)

        assert obj.include_in_schema is result


class TestSubscriber(IncludeTestcase):
    def get_object(self, router: RabbitRouter) -> Any:
        return router.subscriber("test")

    def test_graceful_timeout(self) -> None:
        broker = RabbitBroker(graceful_timeout=10)
        router = RabbitRouter()
        router2 = RabbitRouter()

        obj = self.get_object(router2)

        router.include_router(router2)
        broker.include_router(router)

        assert obj._outer_config.graceful_timeout == 10

    def test_simple_router_prefix(self) -> None:
        broker = RabbitBroker()

        router = RabbitRouter(prefix="1.")
        obj = self.get_object(router)

        broker.include_router(router)

        assert obj._outer_config.prefix == "1."

    def test_nested_router_prefix(self) -> None:
        broker = RabbitBroker()

        router = RabbitRouter(prefix="1.")

        router2 = RabbitRouter(prefix="2.")
        obj = self.get_object(router2)

        router.include_router(router2)
        broker.include_router(router)

        assert obj._outer_config.prefix == "1.2."

    def test_complex_router_prefix(self) -> None:
        broker = RabbitBroker()
        router = RabbitRouter(prefix="1.")

        router2 = RabbitRouter()
        sub2 = self.get_object(router2)

        router3 = RabbitRouter(prefix="5.")
        sub3 = self.get_object(router3)

        router2.include_router(router3, prefix="4.")
        router.include_router(router2)
        broker.include_router(router)

        assert sub2._outer_config.prefix == "1."
        assert sub3._outer_config.prefix == "1.4.5."


class TestPublisher(IncludeTestcase):
    def get_object(self, router: RabbitRouter) -> Any:
        return router.publisher("test")
