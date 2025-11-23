# Copyright (c) 2025 Manos Stoumpos
# Licensed under the MIT License. See LICENSE file in the project root for full license information.

"""
This module contains several custom types used to bypass certain driver restrictions.
"""

import json
import sys
from abc import ABC, abstractmethod
from dataclasses import Field as DataclassField
from dataclasses import is_dataclass
from datetime import date, datetime
from enum import Enum
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Protocol,
    Self,
    TypeVar,
    TypeVarTuple,
    Union,
    Unpack,
    TypeAlias,
    cast,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    TypeAdapter,
    create_model,
)
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import is_pydantic_dataclass
from pydantic_core import core_schema, to_jsonable_python

_BaseQueryTypes: TypeAlias = str | bytes

if sys.version_info >= (3, 14):
    from string.templatelib import Template

    QueryString: TypeAlias = _BaseQueryTypes | Template
else:
    QueryString: TypeAlias = _BaseQueryTypes


class DataclassInstance(Protocol):  # pylint: disable=too-few-public-methods
    """
    A dataclass type protocol.
    """

    __dataclass_fields__: ClassVar[dict[str, DataclassField[Any]]]


ModelClass = BaseModel | DataclassInstance
ModelClassType = type[ModelClass]

STRICT_MODE = True


T = TypeVar("T")
E = TypeVar("E", bound=Enum)
TArgs = TypeVarTuple("TArgs")
A = TypeVar("A")
K = TypeVar("K")
V = TypeVar("V")
M = TypeVar("M", bound=type[BaseModel])


def is_same_type(t1: type, t2: type) -> bool:
    """
    Returns whether `t1` and `t2` refer to the same type
    regardless of any parameterization.

    :param type t1: The first type argument.
    :param type t2: The second type argument.
    """
    return t1 is t2 or get_origin(t1) is t2


def is_model_class(cls: type) -> bool:
    """
    Returns `True` if the given type is either
    a `BaseModel` subclass, a pydantic dataclass,
    or a regular dataclass. Else returns `False`.

    :param type cls: The type to be checked.
    """
    return isinstance(cls, type) and (
        issubclass(cls, BaseModel) or is_dataclass(cls) or is_pydantic_dataclass(cls)
    )


class OnlymapsType(ABC, Generic[T]):
    """
    This is class is to be used as an abstract base class
    for all custom onlymaps types.
    """

    @classmethod
    def factory(
        cls,
        t: type[T],
        field_type_mapper: Callable[[type], type] | None = None,
    ) -> tuple[Union[type["OnlymapsType"], type[T]], Callable[[Any], Any]]:
        """
        Maps a type to its corresponding custom type,
        if such type exists, else returns it as-is
        along with a function used to map objects
        back to their original type.

        :param type t: The type to be mapped.
        :param ((type) -> (type | type)) | None field_type_mapper: A
            field type mapping function used for custom type handling.
        """

        if field_type_mapper:
            t = field_type_mapper(t)

        if is_model_class(t):
            return OnlymapsModel.from_model(cast(ModelClassType, t), field_type_mapper)

        adapter = TypeAdapter(t)

        def map_to_original(obj: Any) -> T:
            jsonable = to_jsonable_python(obj)
            return adapter.validate_python(jsonable, strict=False)

        if t in CLASS_MAP:
            return CLASS_MAP[t], map_to_original

        origin: type = get_origin(t) or t

        if issubclass(origin, Enum):
            return OnlymapsEnum.from_enum(origin), map_to_original

        args = get_args(t)

        if origin is tuple:
            return OnlymapsTuple.from_args(args, field_type_mapper), map_to_original
        if origin is list:
            return OnlymapsList.from_args(args, field_type_mapper), map_to_original
        if origin is set:
            return OnlymapsSet.from_args(args, field_type_mapper), map_to_original
        if origin is dict:
            return OnlymapsDict.from_args(args, field_type_mapper), map_to_original

        return t, lambda x: x

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        Overriden class method.
        """

        # `subcls_param` will be containing the type of the subclass's
        #  single generic parameter, though this type itself will be
        #  unparameterized. For example, in the case of `OnlymapsList`
        #  `subcls_param` will be containing `list[A]` where `A` is unknown.
        subcls: type[OnlymapsType] = getattr(cls, "__orig_bases__")[0]
        subcls_param = get_args(subcls)[0]

        # In case `subcls_param` has itself been further
        # parametrized, said concrete parameters can be found
        # by extracting the arguments of `source_type`.
        if args := get_args(source_type):
            # Special case for model and enum types.
            if (
                origin := get_origin(source_type)
            ) is OnlymapsModel or origin is OnlymapsEnum:
                subcls_param = args[0]
            else:
                subcls_param = subcls_param[*args]

        def parse(value: Any) -> Any:
            """
            The base parsing function.
            """
            try:
                return cls.parse_impl(value, *args)
            except:  # pylint: disable=bare-except
                return value

        base_schema = handler(subcls_param)

        return core_schema.no_info_before_validator_function(parse, base_schema)

    @classmethod
    def _parametrize(
        cls,
        args: tuple[Any, ...],
        field_type_mapper: Callable[[type], type] | None,
    ) -> type[Self]:
        """
        Parametrizes the class with each one of the provided arguments
        after feeding them through the `OnlymapsType.factory` function.

        :param tuple[Any, ...] args: A tuple containing the arguments used
            for the parametrization.
        :param `(type) -> type` | None field_type_mapper: A type mapping
            function that is forwarded to `OnlymapsType.factory`.
        """
        return cls[*(cls.factory(arg, field_type_mapper)[0] for arg in args)]  # type: ignore

    @classmethod
    @abstractmethod
    def parse_impl(cls, value: Any, *args: type) -> Any:
        """
        This is the main parsing function that is to be
        implemented by all subclasses.
        """


class OnlymapsBool(OnlymapsType[bool]):
    """
    Some drivers like MySQL and SQLite do not support
    BOOLEAN types, in place of which they use integers.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        if isinstance(value, int) and not isinstance(value, bool):
            return value == 1
        return value


class OnlymapsStr(OnlymapsType[str]):
    """
    Converts bytes into strings if utf-8 encodable.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value


class OnlymapsBytes(OnlymapsType[bytes]):
    """
    Some drivers like MySQL and SQLite do not support
    BOOLEAN types, in place of which they use integers.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        if isinstance(value, str):
            return value.encode("utf-8")
        return value


class OnlymapsUUID(OnlymapsType[UUID]):
    """
    Some drivers may not natively support UUID.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        if isinstance(value, str):
            return UUID(value)
        return value


class OnlymapsDate(OnlymapsType[date]):
    """
    This class is used so as to parse strings into date
    objects, while also handling non ISO-compliant date strings.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        match value:
            case str():
                dt: datetime = OnlymapsDatetime.parse_impl(value)
                return dt.date()
            case datetime():
                return value.date()
        return value


class OnlymapsDatetime(OnlymapsType[datetime]):
    """
    This class is used so as to parse strings into datetime
    objects, while also handling non ISO-compliant date strings.
    """

    DT_TYPE_ADAPTER = TypeAdapter(datetime, config=ConfigDict(strict=False))

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        match value:
            case str():
                return cls.DT_TYPE_ADAPTER.validate_python(value)
            # NOTE: Match for strictly `date`.
            case date() if not isinstance(value, datetime):
                return datetime(year=value.year, month=value.month, day=value.day)
        return value


class OnlymapsEnum(OnlymapsType[E]):
    """
    This class is used so as to parse either integer
    or string values into their respective enum.
    """

    @classmethod
    def parse_impl(cls, value: Any, *args: type) -> Any:
        enumcls: type[Enum] = args[0]
        return enumcls(value)

    @classmethod
    def from_enum(cls, t: type[Enum]) -> type["OnlymapsEnum"]:
        """
        Given a tuple of argument types, returns a
        correspondingly parametrized `OnlymapsEnum` type.
        """
        return OnlymapsEnum[t]  # type: ignore


class OnlymapsList(OnlymapsType[list[A]]):
    """
    This class is used so as to parse JSON strings into lists.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        match value:
            case str() if value.startswith("[") and value.endswith("]"):
                return json.loads(value)
            case tuple():
                return list(value)
            case _:
                return value

    @classmethod
    def from_args(
        cls, args: tuple[type, ...], field_type_mapper: Callable[[type], type] | None
    ) -> type["OnlymapsList"]:
        """
        Given a tuple of argument types, returns a
        correspondingly parametrized `OnlymapsList` type.
        """
        if args:
            assert len(args) == 1
            return cls._parametrize(args, field_type_mapper)
        return OnlymapsList[Any]


class OnlymapsTuple(OnlymapsType[tuple[Unpack[TArgs]]], Generic[Unpack[TArgs]]):
    """
    This class is used so as to parse JSON strings into tuples.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        match value:
            case str() if isinstance(l := OnlymapsList.parse_impl(value), list):
                return tuple(l)
            case _:
                return value

    @classmethod
    def from_args(
        cls, args: tuple[type, ...], field_type_mapper: Callable[[type], type] | None
    ) -> type["OnlymapsTuple"]:
        """
        Given a tuple of argument types, returns a
        correspondingly parametrized `OnlymapsTuple` type.
        """
        if args:
            return cls._parametrize(args, field_type_mapper)
        return OnlymapsTuple[Any, ...]  # type: ignore


class OnlymapsSet(OnlymapsType[set[A]]):
    """
    This class is used so as to parse JSON strings into sets.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        match value:
            case tuple():
                return set(value)
            case str() if isinstance(l := OnlymapsList.parse_impl(value), list):
                return set(l)
            case _:
                return value

    @classmethod
    def from_args(
        cls, args: tuple[type, ...], field_type_mapper: Callable[[type], type] | None
    ) -> type["OnlymapsSet"]:
        """
        Given a tuple of argument types, returns a
        correspondingly parametrized `OnlymapsSet` type.
        """
        if args:
            assert len(args) == 1
            return cls._parametrize(args, field_type_mapper)
        return OnlymapsSet[Any]


class OnlymapsDict(OnlymapsType[dict[K, V]]):
    """
    This class is used so as to parse JSON strings into dictionaries.
    """

    @classmethod
    def parse_impl(cls, value: Any, *_: type) -> Any:
        if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
            return json.loads(value)
        return value

    @classmethod
    def from_args(
        cls, args: tuple[type, ...], field_type_mapper: Callable[[type], type] | None
    ) -> type["OnlymapsDict"]:
        """
        Given a tuple of argument types, returns a
        correspondingly parametrized `OnlymapsDict` type.
        """
        if args:
            assert len(args) == 2
            return cls._parametrize(args, field_type_mapper)
        return OnlymapsDict[str, Any]


class OnlymapsModel(OnlymapsType[M]):
    """
    This class is used to parse JSON strings into Pydantic models.
    """

    @classmethod
    def parse_impl(cls, value: Any, *args: type) -> Any:
        pydcls: type[BaseModel] = args[0]
        if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
            return pydcls.model_validate_json(value, strict=STRICT_MODE)
        return value

    @classmethod
    def from_model(
        cls,
        t: ModelClassType,
        field_type_mapper: Callable[[type], type] | None = None,
    ) -> tuple[type["OnlymapsModel"], Callable[[BaseModel], ModelClass]]:
        """
        Receives either a `BaseModel` or a dataclass type object and
        returns a tuple containing two objects:

        The first object is an `OnlymapsModel` type whose underlying `BaseModel`
        class schema corresponds to the schema of `cls`, though its field types
        have been recurively run through the `OnlymapsType.factory` function so
        as to convert them into their equivalent `OnlymapsType` type, after first
        running them through the `field_type_mapper` function, if one was provided.

        The second object is a function that, given a `BaseModel` instance that has
        been validated via `OnlymapsType` mentioned aboved, returns an instance of
        the original type. This function only exists for types like dataclasses which
        do not perform validation, and therefore are unable to deserialize any serialized
        values, nor revert any mappings applied to their field types.

        :param `ModelClassType` cls: The model class that is to be customized.
        :param ((type) -> (type | type)) | None field_type_mapper: A
            field type mapping function used for custom type handling.
        """

        model_fields = (
            pydantic_dataclass(t) if is_dataclass(t) else t
        ).__pydantic_fields__

        original_fields: dict[str, Any] = {}
        mapped_fields: dict[str, Any] = {}

        for name, field_info in model_fields.items():
            field_type = cast(type, field_info.annotation)

            original_fields[name] = (
                field_type,
                Field(field_info.default),
            )

            if field_type_mapper:
                field_type = field_type_mapper(field_type)

            mapped_fields[name] = (
                OnlymapsType.factory(field_type, field_type_mapper)[0],
                Field(default=field_info.default),
            )

        # NOTE: Use the same exact name for the custom model
        #       so that any raised exceptions mention this name.
        custom_model = create_model(t.__name__, __base__=BaseModel, **mapped_fields)
        original_model = create_model(
            "Original" + t.__name__, __base__=BaseModel, **original_fields
        )

        om_model = OnlymapsModel[custom_model]  # type: ignore

        def map_to_original(model: BaseModel) -> ModelClass:
            # This is necessary for any dataclass models that may
            # contain any pydantic model fields. Due to pydantic's
            # tendency to recursively call `model_dump`, any pydantic
            # fields will be converted into dictionaries, and there would
            # be no way to parse them back into their original models,
            # since dataclasses perform no validation whatsoever.
            data = model.model_dump()
            parsed_as_original = original_model(**data)
            return t(**dict(parsed_as_original))

        return (om_model, map_to_original)


CLASS_MAP: dict[type, type[OnlymapsType]] = {
    # NOTE: Do not include `OnlymapsBool` by default
    #       as many drivers can handle booleans.
    #       Instead, let each driver handle it.
    str: OnlymapsStr,
    bytes: OnlymapsBytes,
    UUID: OnlymapsUUID,
    date: OnlymapsDate,
    datetime: OnlymapsDatetime,
}
