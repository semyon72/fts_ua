# IDE: PyCharm
# Project: fts_ua
# Path: fts_sqlite/tests
# File: test_attach_capability.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-12-26 (y-m-d) 7:39 AM

import os
import sqlite3 as sqlite
import unittest

from fts_sqlite.blog_sqlite_fts import attach
import fts_sqlite.tests.attach_capability_base as capability
import fts_sqlite.tests.con_util as conutil


class TestInMemoryFTSAttachedCapability(capability.AttachCapabilityBase):

    attach_as = 'fts_index'

    def get_con(self) -> sqlite.Connection:
        con_url = 'file:memorydb_blog?mode=memory'
        return conutil.traceback(sqlite.connect(con_url, uri=True), 'InMem-CONTENT')

    def get_fts_con(self) -> sqlite.Connection:
        self.attach_url = 'file:memorydb_blog_index?mode=memory&cache=shared'
        return conutil.traceback(sqlite.connect(self.attach_url, uri=True), 'InMem-FTS-INDEX')

    def test_1_attach_no_trigger_insertion(self):
        self.create_schema()
        self.create_fts_schema()

        attach(self.con, self.attach_url, self.attach_as, True)
        self.are_tables_reachable(self.con, self.attach_as, self.get_fts_schema().keys())

        self.insert_data()

    def test_2_no_attach_with_trigger_insertion(self):
        self.create_schema()
        self.create_insert_trigger(self.con)

        self.create_fts_schema()
        self.insert_data()

        self.indexed_data_test(True)

    def test_3_attach_with_trigger_insertion(self):
        self.create_schema()
        self.create_insert_trigger(self.con)

        self.create_fts_schema()

        attach(self.con, self.attach_url, self.attach_as)
        self.are_tables_reachable(self.con, self.attach_as, self.get_fts_schema().keys())

        self.insert_data()
        self.indexed_data_test(True)


class TestInFileContentAttachedCapability(capability.AttachCapabilityBase):

    @staticmethod
    def reset_db(file):
        if os.path.isfile(file):
            os.remove(file)

    def get_con(self) -> sqlite.Connection:
        self.work_dir = '/home/ox23/PycharmProjects/fts_ua/.work/'
        self.con_db_file = f'{self.work_dir}blog_content.sqlite3'
        self.reset_db(self.con_db_file)
        self.attach_url = f'file:{self.con_db_file}'
        return conutil.traceback(sqlite.connect(self.attach_url), 'InFile-CONTENT:')

    def get_fts_con(self) -> sqlite.Connection:
        self.fts_db_file = f'{self.work_dir}blog_fts_index.sqlite3'
        self.reset_db(self.fts_db_file)
        fts_url = f'file:{self.fts_db_file}'
        return conutil.traceback(sqlite.connect(fts_url), 'InFile-FTS-INDEX:')

    def test_1_attach_no_trigger_insertion(self):
        self.create_schema()
        self.create_fts_schema()

        attach(self.fts_con, self.attach_url, self.attach_as, True)
        self.are_content_tables_reachable(self.fts_con, self.attach_as)

        self.insert_data()
        self.check_data(self.fts_con, self.attach_as)

    def test_2_no_attach_with_trigger_insertion(self):
        self.create_schema()
        self.create_insert_trigger(self.con)

        self.create_fts_schema()

        self.insert_data()
        self.indexed_data_test(True)

    def test_3_attach_with_trigger_insertion(self):
        self.create_schema()
        self.create_insert_trigger(self.con)

        self.create_fts_schema()

        attach(self.fts_con, self.attach_url, self.attach_as)
        self.are_content_tables_reachable(self.fts_con, self.attach_as)

        self.insert_data()
        self.check_data(self.fts_con, self.attach_as)

        self.indexed_data_test(True)


@unittest.skip('It hangs up. Looks like a bug because the same things for files work perfect')
class TestInMemoryContentAttachedCapability(TestInFileContentAttachedCapability):

    def reset_db(self):
        raise AssertionError('Redundant method from parent. It should\'t be used')

    def get_con(self) -> sqlite.Connection:
        self.attach_url = 'file:memorydb_blog?mode=memory&cache=shared'
        return conutil.traceback(sqlite.connect(self.attach_url, uri=True), 'InMem-CONTENT')

    def get_fts_con(self) -> sqlite.Connection:
        fts_url = 'file:memorydb_blog_index?mode=memory'
        return conutil.traceback(sqlite.connect(fts_url, uri=True), 'InMem-FTS-INDEX')

