from __future__ import annotations

from typing import Any, Callable, List, Union

from dash import Dash

from cars import LOG


class deferred_registry:
    REGISTRY: List[AnyDeferred] = []

    @classmethod
    def add(cls, cb: AnyDeferred) -> None:
        cls.REGISTRY.append(cb)

    @classmethod
    def apply(cls, app: Dash) -> None:

        for cb in cls.REGISTRY:
            try:
                if isinstance(cb, deferred_callback):
                    LOG.info(f"Applying deferred callback {cb.f.__name__}")
                    app.callback(*cb.args, **cb.kwargs)(cb.f)
                else:
                    LOG.info(f"Applying deferred clientside callback {cb.name}")
                    app.clientside_callback(*cb.args, **cb.kwargs)
            except Exception as e:
                LOG.error(
                    f"Callback {type(cb)=}, {cb=}, "
                    f"{getattr(cb, 'name', None)=}"
                    f"{getattr(cb, 'f', None)=}"
                    f"failed"
                )
                LOG.error(e)
                continue


class deferred_callback:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f: Callable[..., Any]) -> None:
        self.f = f
        deferred_registry.add(self)


class deferred_clientside_callback:
    def __init__(
        self,
        name: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> None:

        self.name = name
        self.args = args
        self.kwargs = kwargs

        deferred_registry.add(self)


AnyDeferred = Union[deferred_callback, deferred_clientside_callback]
