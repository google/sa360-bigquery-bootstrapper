import app_flags
import flagmaker.settings

test = flagmaker.settings.SettingOption.create("Hint")
test.value = "Test"

assert(test[0].isalnum())
assert(test[-1].isalnum())
assert(test[0] == "T")
assert(test[-1] == "t")