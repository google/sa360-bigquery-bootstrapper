try:
  import bootstrapper
except ImportError:
  from pip._internal.utils import subprocess
  subprocess.call_subprocess(['pipenv', 'install'], show_stdout=False)
  import bootstrapper

from absl import flags
import app_flags as fl
assigned_flags = fl.assign_flags()
flags.adopt_module_key_flags(fl)

if __name__ == '__main__':
  bootstrapper.Bootstrap(assigned_flags)
