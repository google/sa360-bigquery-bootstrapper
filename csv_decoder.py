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

"""
Decodes CSV files. If given a folder, recursively opens.

Handles ZIP files.
"""
import os
import shutil
import tarfile
import zipfile
from datetime import datetime

import pandas as pd
from absl import logging
from xlrd import XLRDError


class Decoder(object):
    def __init__(self, desired_encoding, path):
        self.desired_encoding = desired_encoding
        self.path = path
        self.dest = None
        self._file_count = 0

    @property
    def file_count(self):
        self._file_count += 1
        return self._file_count

    def run(self):
        self.dest = '/tmp/updir-' + self.time
        os.mkdir(self.dest)
        Decoder.ChooseyDecoder(self, self.path).run()
        return self.dest

    @property
    def time(self) -> str:
        return str(datetime.now().strftime('%Y%m%d%H%m%S%f'))

    class AbstractDecoder(object):
        def __init__(self, parent: 'Decoder', path: str):
            self.path = path
            self.parent = parent

        def open(self) -> bytes:
            with open(self.path, 'rb') as fh:
                return fh.read()

    class ChooseyDecoder(AbstractDecoder):
        def run(self):
            if os.path.isdir(self.path):
                return Decoder.DirectoryDecoder(self.parent, self.path).run()
            elif self.path.endswith('.csv') or self.path.endswith('.xlsx'):
                return Decoder.FileDecoder(self.parent, self.path).run()
            elif tarfile.is_tarfile(self.path):
                return Decoder.TarfileDecoder(self.parent, self.path).run()
            elif zipfile.is_zipfile(self.path):
                return Decoder.ZipfileDecoder(self.parent, self.path).run()
            else:
                logging.info('Skipping ' + self.path)

    class DirectoryDecoder(AbstractDecoder):
        def run(self):
            for path in os.listdir(self.path):
                p = self.path + '/' + path
                Decoder.ChooseyDecoder(
                    self.parent, p
                ).run()

    class FileDecoder(AbstractDecoder):
        def run(self):
            try:
                df = pd.read_excel(self.path)
                with open('{}/{}.csv'.format(
                    self.parent.dest, self.parent.file_count
                ), 'wb') as fh:
                    contents = bytes(
                        df.to_csv(index=False), self.parent.desired_encoding
                    )
                    fh.write(contents)
                    logging.info('Wrote to ' + self.path)
                return
            except XLRDError as err:
                self.decode(self.open())
                return

        def decode(self, file: bytes):
            for encoding in ['utf-8', 'utf-16', 'latin-1']:
                try:
                    contents = self.decode_file(encoding, file)
                    with open('{}/{}.csv'.format(
                        self.parent.dest, self.parent.file_count
                    ), 'wb') as fh:
                        fh.write(contents)
                    break
                except UnicodeDecodeError:
                    logging.info('Unicode error for ' + self.path)

        def decode_file(self, encoding: str, file: bytes):
            return file.decode(encoding).encode(self.parent.desired_encoding)

    class TarfileDecoder(AbstractDecoder):
        def run(self):
            with tarfile.open(self.path) as th:
                extraction_directory = '/tmp/tar-output-' + self.parent.time
                th.extractall(extraction_directory)
                Decoder.DirectoryDecoder(self.parent, extraction_directory).run()
                shutil.rmtree(extraction_directory)

    class ZipfileDecoder(AbstractDecoder):
        def run(self):
            with zipfile.ZipFile(self.path, 'r') as zh:
                extraction_directory = '/tmp/zip-output-' + self.parent.time
                zh.extractall(extraction_directory)
                Decoder.DirectoryDecoder(
                    self.parent, extraction_directory
                ).run()
                shutil.rmtree(extraction_directory)
