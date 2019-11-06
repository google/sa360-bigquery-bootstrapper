# Lint as: python3
"""TODO(seancjones): DO NOT SUBMIT without one-line documentation for test.

TODO(seancjones): DO NOT SUBMIT without a detailed description of test.
"""

from absl import app
from absl import flags

flags.DEFINE_list('l', default=None, help='list')
FLAGS = flags.FLAGS


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')
  print(FLAGS.l)
if __name__ == '__main__':
  app.run(main)
