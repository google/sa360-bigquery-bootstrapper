from abc import ABC
from abc import abstractmethod
from absl import flags
from termcolor import cprint
from typing import ClassVar

from .exceptions import FlagMakerConfigurationError
from .exceptions import FlagMakerInputError
from .sanity import Validator
from .settings import SettingOption
from .settings import SettingOptions
from .building_blocks import SettingsInterface

FLAGS = flags.FLAGS


class AbstractSettings(SettingsInterface):
    """Settings Base Class

    Loaded from the Config class. Used to generate flags for an app.
    """

    args: dict = None

    def start(self):
        """Bootstraps the settings loading process

        Called from load_settings. Should not be called directly.
        """
        for k in self.args:
            self.args[k].set_value(init=FLAGS.get_flag_value(k, None))

    def load_settings(self):
        self.start()
        first = True
        interactive_mode = self.args.pop('interactive')
        for k in self.args.keys():
            setting: SettingOption = self.args[k]
            if setting.maybe_needs_input():
                if not interactive_mode and setting.default:
                    setting._value.set_val(setting.default)
                    continue
                if first:
                    cprint('Interactive Setup', attrs=['bold'])
                    first = False
                if setting.include_in_interactive and interactive_mode:
                    setting.set_value(prompt=setting.get_prompt(k))
        return self

    def assign_flags(self) -> flags:
        for k in self.args:
            kwargs = {
                'default': None,
            }
            if not self.args[k].include_in_interactive:
                kwargs['default'] = self.args[k].default
            self.args[k].method(k, help=self.args[k].help, **kwargs)
        return FLAGS

    def __getitem__(self, item):
        return self.args[item]

    def get_settings(self):
        settings = self.settings()
        # system settings
        settings += {
            'interactive': SettingOption.create(
                self,
                'Interactive Mode',
                default=False,
                include_in_interactive=False,
                method=flags.DEFINE_bool,
            ),
        }
        return settings

    def install(self):
        self.args = self.get_settings()
        self.assign_flags()

    def __repr__(self):
        return str(self.args)


AbstractSettingsClass = ClassVar[AbstractSettings]


class Config(object):
    """The entry point for setting the Settings class for an app.

    Example: Config(MySettingsClass)

    This will bootstrap the settings class correctly.
    """
    def __init__(self, s: AbstractSettingsClass):
        self.s = s
        self.instance = None
        self.instance: AbstractSettings = s()
        self.instance.install()

    def get(self) -> AbstractSettings:
        if not FLAGS.is_parsed():
            raise FlagMakerConfigurationError('Do not call this '
                                              'method until after app.run()')
        self.instance.load_settings()
        return self.instance
