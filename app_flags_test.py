import app_flags

test = app_flags.SimpleFlag.create("Hint")
test.value = "Test"

assert(test[0].isalnum())
assert(test[-1].isalnum())
assert(test[0] == "T")
assert(test[-1] == "t")