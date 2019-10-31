from abc import ABC
from abc import abstractmethod
from absl import flags
from termcolor import cprint
from typing import ClassVar

from flagmaker.exceptions import FlagMakerConfigurationError
from .exceptions import FlagMakerInputError
from .sanity import Validator
from .settings import SettingOption
from .settings import SettingOption
from .settings import SettingOptions

FLAGS = flags.FLAGS


class AbstractSettings(SettingOptions, ABC):
    """Settings Base Class

    Loaded from the Config class. Used to generate flags for an app.
    """

    args: dict = None

    def start(self):
        """Bootstraps the settings loading process

        Called from load_settings. Should not be called directly.
        """
        for k in self.args.keys():
            self.args[k].set_value(init=FLAGS.get_flag_value(k, None))

    @abstractmethod
    def settings(self) -> dict:
        pass

    @classmethod
    def load_settings(cls):
        s: AbstractSettings = cls()
        first = True
        for k in s.keys():
            setting: SettingOption = s[k]
            if setting.maybe_needs_input():
                if first:
                    cprint('Interactive Setup', attrs=['bold'])
                    first = False
                setting.set_value(prompt=setting.get_prompt(k))
        s.args: dict = s.settings()
        s.assign_flags()
        return s

    def assign_flags(self) -> flags:
        for k in self.args:
            self.args[k].method(k, None, self.args[k].help)
        return FLAGS


AbstractSettingsClass = ClassVar[AbstractSettings]


class Config(object):
    """The entry point for setting the Settings class for an app.

    Example: Config(MySettingsClass)

    This will bootstrap the settings class correctly.
    """
    def __init__(self, s: AbstractSettingsClass):
        self.instance = s.load_settings()

    def get(self) -> AbstractSettings:
        if not FLAGS.is_parsed():
            raise FlagMakerConfigurationError('Do not call this '
                                              'method until after app.run()')
        self.instance.start()
        return self.instance
