from absl import flags

import flagmaker.settings
import app_flags

settings = app_flags.AppSettings()
test = flagmaker.settings.SettingOption.create(settings, "Hint")
test.value = "Test"

assert(test[0].isalnum())
assert(test[-1].isalnum())
assert(test[0] == "T")
assert(test[-1] == "t")


def test_bool(value, check):
    boolean = flagmaker.settings.SettingOption.create(settings,
                                                      "Hint",
                                                      method=flags.DEFINE_bool)


    boolean.set_value(value=value)
    assert(boolean.value == check)

test_bool('true', True)
test_bool('1', True)
test_bool(True, True)
test_bool('True', True)

test_bool('false', False)
test_bool('0', False)
test_bool(False, False)
test_bool('False', False)

