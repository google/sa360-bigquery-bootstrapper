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
from typing import Union

import numpy as np
import os
import shutil
import tarfile
import zipfile

from termcolor import cprint
from datetime import datetime
import pandas as pd
from absl import logging
from typing import Dict
from xlrd import XLRDError

from exceptions import ReadError
from flagmaker.settings import SettingOption
from utilities import Locale


class Decoder(object):
    SINGLE_FILE = 1
    SEPARATE_FILES = 2

    def __init__(self, desired_encoding, path, locale: Locale,
                 dict_map: Dict[str, SettingOption], thousands=',',
                 out_type=SINGLE_FILE, dest='out.csv', callback=None):
        self.locale = locale
        self.possible_delimiters: list = [',', '\t', '|',]
        self.first = True  # ignore headers when False
        self.map = {}
        self.dtypes = {}
        self.errors_found = []
        self.columns = []
        self.parse_dates = []
        for k, v in dict_map.items():
            k = k.lower()
            self.columns.append(v.value.lower())
            self.map[k] = v.default
            if v.attrs['dtype'] == np.datetime64:
                self.parse_dates.append(v.default)
                self.dtypes[v.default] = np.object
            else:
                self.dtypes[v.default] = v.attrs['dtype']

        self.out_type = out_type
        self.desired_encoding = desired_encoding
        self.thousands = thousands
        self.path = path
        self.dest = dest
        self._file_count = 0
        self.dir = None
        self.rows_opened: int = 0
        self.callback = callback

    def reorder_delimiters(self, new_start):
        self.possible_delimiters = [
            self.possible_delimiters.pop(new_start)
        ] + self.possible_delimiters

    @property
    def filename(self):
        if self.out_type == Decoder.SINGLE_FILE:
            return self.dest
        self._file_count += 1
        return self._file_count

    def run(self):
        self.dir = '/tmp/sa-bq-updir'
        file = self.dir + '/' + self.dest
        if os.path.exists(file):
            os.remove(file)
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        Decoder.ChooseyDecoder(self, self.path).run()

        if len(self.errors_found) > 0:
            cprint('The following formatting errors were found:', 'red')
            for error in self.errors_found:
                cprint('- {}'.format(error), 'red')
            cprint('Please correct these errors and re-run.', 'red')
            exit(1)
        return (
            self.dir
            if self.out_type != Decoder.SINGLE_FILE
            else '{}/{}'.format(
                self.dir, self.dest
            )
        )

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if os.path.exists(self.dir + '/' + self.dest):
            os.remove(self.dir + '/' + self.dest)
        pass

    @property
    def time(self) -> str:
        return str(datetime.now().strftime('%Y%m%d%H%m%S%f'))

    class AbstractDecoder(object):
        def __init__(self, parent: 'Decoder', path: str):
            self.path = path
            self.parent = parent

    class ChooseyDecoder(AbstractDecoder):
        def run(self):
            if os.path.isdir(self.path):
                cprint('Decoding directory')
                return Decoder.DirectoryDecoder(self.parent, self.path).run()
            elif self.path.endswith('.csv') or self.path.endswith('.xlsx'):
                cprint('Decoding file...', 'grey')
                return Decoder.FileDecoder(self.parent, self.path).run()
            elif tarfile.is_tarfile(self.path):
                cprint('Unpacking tar file')
                return Decoder.TarfileDecoder(self.parent, self.path).run()
            elif zipfile.is_zipfile(self.path):
                cprint('Unzipping zip file')
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
                self.decode_excel()
            except XLRDError as err:
                self.decode_csv()
                return

        def read(
            self,
            method: Union[pd.read_csv, pd.read_excel],
            path, dtype, **kwargs
        ) -> pd.DataFrame:
            i = 0
            for i in range(len(self.parent.possible_delimiters)):
                sep = self.parent.possible_delimiters[i]
                try:
                    res = self._read(method, path, dtype, sep=sep, **kwargs)
                    if i > 0:
                        self.parent.reorder_delimiters(i)
                    return res
                except ReadError:
                    continue
            raise ReadError('Could not recognize delimiter. '
                            'Please save as one of: \n'
                            '- Comma Separated Values\n'
                            '- Tab Separated Values\n'
                            '- Pipe Separated Values\n'
                            '- Excel spreadsheet\n'
                            'If you think this is an error, please first try '
                            'to re-save this file in one of these formats '
                            'and then try again.')

        def _read(
            self,
            method: Union[pd.read_csv, pd.read_excel],
            path, dtype, **kwargs
        ) -> pd.DataFrame:
            i = 0

            def rename_columns(s: str):
                nonlocal i
                ls = s.lower()
                if ls not in self.parent.columns:
                    i += 1
                    logging.debug('[no column named {}]'.format(ls))
                    return 'empty{}'.format(i)
                rn = self.parent.map[ls] if ls in self.parent.map else ls
                logging.debug('renamed column {} to {}'.format(ls, rn))
                return rn
            nrows = kwargs.get('nrows', 0)
            kwargs['nrows'] = 0
            hdf: pd.DataFrame = method(path, **kwargs)
            logging.debug('Checking path %s', path)
            logging.debug('Headers: %s', list(hdf))
            try:
                hdf.rename(
                    rename_columns, inplace=True,
                    copy=False, axis='columns', errors='raise'
                )
                headers = list(hdf.columns)
                del kwargs['nrows']
                kwargs['skiprows'] = kwargs.get('skiprows', 0) + 1
                if len(headers) != len(set(headers)):
                    self.parent.errors_found.append(
                        'Duplicate columns in {}'.format(self.path)
                    )
                if nrows > 0:
                    kwargs['nrows'] = nrows
                if len(self.parent.errors_found) == 0:
                    df = method(path, dtype=dtype, names=headers, **kwargs)
                    arguments = {
                        'dayfirst': self.parent.locale != Locale.US,
                    }
                    df['date'] = pd.to_datetime(df['date'], **arguments)
                    return df
                return pd.DataFrame()
            except KeyError as err:
                raise ReadError(err)

        def decode_excel(self):
            df: pd.DataFrame = self.read(
                pd.read_excel,
                self.path,
                dtype=self.parent.dtypes,
                parse_dates=self.parent.parse_dates,
                thousands=self.parent.thousands,
            )
            logging.info('Converted XLSX file {} to CSV'.format(self.path))
            return df

        def decode_csv(self):
            for encoding in ['utf-8', 'utf-16', 'latin-1']:
                try:
                    df = self.read(
                        pd.read_csv,
                        self.path,
                        encoding=encoding,
                        dtype=self.parent.dtypes,
                        thousands=self.parent.thousands,
                    )
                    if len(self.parent.errors_found) == 0:
                        self.write(df)
                        logging.info('Decoded %s from %s',
                                     self.path.replace('//', '/'), encoding)
                    break
                except (UnicodeDecodeError, UnicodeError):
                    if encoding == 'latin-1':
                        raise
                    logging.info(
                        'Unicode error for %s with %s',
                        self.path,
                        encoding
                    )

        def write(self, df: pd.DataFrame):
            doing_single_file = self.parent.out_type == Decoder.SINGLE_FILE
            if doing_single_file:
                write_method = 'a'
                include_headers = self.parent.first
                if self.parent.first:
                    self.parent.first = False
            else:
                include_headers = True
                write_method = 'w'
            self.parent.rows_opened += len(df.index)
            cprint(
                '+ Stored a file {}'.format(self.parent.filename), 
                'green'
            )
            df.to_csv(
                '{}/{}'.format(self.parent.dir, self.parent.filename),
                index=False,
                header=include_headers,
                mode=write_method,
                columns=self.parent.map.values(),
                date_format='%Y-%m-%d'
            )

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
