# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains SQL parameter wrapper classes.
"""

from abc import ABC
from typing import Any, Callable, Mapping, Sequence

from pydantic import BaseModel

__all__ = ["Bulk", "Json"]


class _ParamWrapper(ABC, BaseModel):
    """
    A base class for all SQL parameter wrapper classes.
    """

    def __init__(self, obj: Any, /):
        """
        Wraps the provided object parameter.

        :param Any obj: The object parameter to be wrapped.
        """
        super().__init__()
        self.__obj = obj

    @property
    def value(self) -> Any:
        """
        Returns the wrapper's underlying object.
        """
        return self.__obj


class Bulk(_ParamWrapper):
    """
    Marks the provided object so that it be used as part of a bulk statement.
    """

    def __init__(self, obj: Sequence[Sequence[Any] | Mapping[str, Any]], /):
        """
        Wraps the provided object parameter.

        :param Any obj: The object parameter to be wrapped.
        """
        super().__init__(obj)

    def get_mapped_value(self, arg_map_fn: Callable[[Any], Any]) -> Any:
        """
        Returns this `Bulk` instance's underlying value after having
        each of its items go through the provided mapping function.

        :param `(Any) -> Any` arg_map_fn: An argument mapping function.

        """

        bulk_type = type(self.value)

        if issubclass(bulk_type, (list, tuple, set)):

            def handle_seq_or_map_param(p: Any) -> Any:
                match p:
                    case dict():
                        return {key: arg_map_fn(val) for key, val in p.items()}
                    case list() | tuple() | set():
                        return type(p)(arg_map_fn(val) for val in p)
                    case _:
                        return p

            return bulk_type(handle_seq_or_map_param(p) for p in self.value)

        return self.value


class Json(_ParamWrapper):
    """
    Marks the provided object so that it be converted into a JSON string.
    """

    def __init__(self, obj: Any, /):  # pylint: disable=useless-parent-delegation
        """
        Wraps the provided object parameter.

        :param Any obj: The object parameter to be wrapped.
        """
        super().__init__(obj)
