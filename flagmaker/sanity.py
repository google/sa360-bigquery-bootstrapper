from absl import flags
from termcolor import cprint

from .hints import StringList
from .building_blocks import SettingOptionInterface


class Validator:
    @staticmethod
    def check_bool(setting: SettingOptionInterface, errors: list):
        value = setting.value
        if isinstance(value, bool):
            return
        if value in ('true', 'True', 'false', 'False', '0', '1', True, False):
            return
        errors.append('Invalid option {}'.format(setting.value)
                      + '. Expecting true/false or 0/1')

    @staticmethod
    def validate(s: SettingOptionInterface) -> bool:
        if s.get_method() == flags.DEFINE_boolean and s.validation is None:
            s.validation = Validator.check_bool
        if s.validation is not None:
            errors: StringList = []
            s.validation(s, errors)
            if len(errors) > 0:
                cprint('Error{0}:'.format(
                    's' if len(errors) > 1 else ''
                ), 'red', attrs=['bold'])
                cprint('\n'.join(map(lambda x: '-' + x, errors)), 'red')
                return False
        return True

