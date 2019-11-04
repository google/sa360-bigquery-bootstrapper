from abc import ABC
from abc import abstractmethod
from abc import abstractproperty
from enum import Enum


class SettingOptionInterface(ABC, object):
    @abstractmethod
    def get_method(self):
        pass

    @property
    @abstractmethod
    def value(self):
        pass

    @value.setter
    @abstractmethod
    def value(self, val):
        pass


class ValueType(Enum):
    """
    ENUM to allow multiple value types for the type-safe Value class
    """
    STRING = 0
    INTEGER = 1
    BOOLEAN = 2
    LIST = 3


class Value(object):
    """
    A type-safe Value class to hold settings values.
    """
    value_type: ValueType = ValueType.STRING
    b_val: bool = None
    i_val: int = None
    s_val: str = None
    l_val: list = None
    __value_set: bool = False

    def set_val(self, val):
        t = type(val)

        def set():
            if t is int:
                self.i_val = val
                self.value_type = ValueType.INTEGER
                return
            if t is str:
                self.s_val = val
                return
            if t is bool:
                self.b_val = val
                self.value_type = ValueType.BOOLEAN
            if t is list:
                self.l_val = val
                self.value_type = ValueType.LIST
        set()
        self.__value_set = val != '' and val is not None

    def get_val(self):
        if self.value_type == ValueType.STRING:
            return self.s_val
        if self.value_type == ValueType.BOOLEAN:
            return self.b_val
        if self.value_type == ValueType.INTEGER:
            return self.i_val
        if self.value_type == ValueType.LIST:
            return self.l_val

    def __bool__(self):
        return self.__value_set


class SettingsInterface(ABC):
    custom = {}

    @abstractmethod
    def load_settings(self):
        pass

    @abstractmethod
    def settings(self) -> dict:
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def __getitem__(self, item):
        pass