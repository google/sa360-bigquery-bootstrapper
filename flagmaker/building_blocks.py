# /***********************************************************************
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Note that these code samples being shared are not official Google
# products and are not formally supported.
# ************************************************************************/
from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable
from enum import Enum
from typing import Generic
from typing import Optional

from typing import TypeVar

from typing import List


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


T = TypeVar('T')


class SettingsInterface(ABC, Generic[T]):
    custom = {}

    @abstractmethod
    def load_settings(self) -> T:
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


def list_to_string_list(l: Optional[List]):
    i = [0]

    def count():
        i[0] += 1
        return i[0]

    return '\n'.join(list(map(lambda x: '{}. {}'.format(count(), x), l)))