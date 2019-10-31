try:
    import bootstrapper
except ImportError:
    from pip._internal.utils import subprocess
    subprocess.call_subprocess(['pipenv', 'install'], show_stdout=False)
    import bootstrapper

from absl import flags
import flagmaker as fl

if __name__ == '__main__':
    bootstrap = bootstrapper.Bootstrap()
    flags.adopt_module_key_flags(fl)
    bootstrap.run()
