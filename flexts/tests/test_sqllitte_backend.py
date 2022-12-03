# IDE: PyCharm
# Project: fts_ua
# Path: ${DIR_PATH}
# File: ${FILE_NAME}
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-28 (y-m-d) 10:09 AM
from unittest import TestCase

import sqlite3 as sqlite

from flexts.sqllitte_backend import InsertTrigger, TriggerBase, UpdateTrigger, DeleteTrigger, Trigger
from flexts.tests.test_sqlite_fts5 import SQLiteFTS5Util


class TestCompliance(TestCase):

    def setUp(self) -> None:
        self.con_src = sqlite.connect(':memory:')
        self.con_dest = sqlite.connect(':memory:')
        self.tbl_src_name = 'test_src'
        self.tbl_dst_name = 'test_dest'

        self.init_all()

    def tearDown(self) -> None:
        self.con_src.close()
        self.con_dest.close()

    def replicate(self, *args):

        action = args[-1]
        cursor = self.con_dest.cursor()
        try:
            if action == 'new':
                sql = f'INSERT INTO {self.tbl_dst_name} (id, title, "text") VALUES (?, ?, ?)'
                prms = args[:-1]
            elif action == 'delete':
                sql = f'DELETE FROM {self.tbl_dst_name} WHERE id = ?'
                prms = args[:1]
            elif action == 'update':
                sql = f'UPDATE {self.tbl_dst_name} SET title = ?, "text" = ? WHERE id = ?'
                prms = [*args[4:-1], args[3]]
            else:
                raise ValueError(f'unknown action "{action}"')
            try:
                cursor.execute(sql, prms)
            except sqlite.Error as exc:
                raise exc

        finally:
            cursor.close()

        return cursor.rowcount == 1

    def init_all(self):
        self.con_src.create_function('replicate', -1, self.replicate)

        sqls = (
            f'CREATE TABLE {self.tbl_src_name} (id INTEGER PRIMARY KEY, title TEXT, "text" TEXT);',
            f'CREATE TRIGGER {self.tbl_src_name}_ai AFTER INSERT ON {self.tbl_src_name} BEGIN'
            ' SELECT replicate(new.id, new.title, new.text, \'new\'); END;',
            f'CREATE TRIGGER {self.tbl_src_name}_ad AFTER DELETE ON {self.tbl_src_name}'
            ' BEGIN SELECT replicate(old.id, old.title, old.text, \'delete\'); END;',
            f'CREATE TRIGGER {self.tbl_src_name}_au AFTER UPDATE ON {self.tbl_src_name}'
            ' BEGIN SELECT replicate(old.id, old.title, old.text, new.id, new.title, new.text, \'update\'); END;',
        )

        cursor = self.con_src.cursor()
        for i, sql in enumerate(sqls):
            with self.subTest(source_sql_i=i):
                cursor.execute(sql)
                pass
        cursor.close()

        # destination part
        sql_d = f'CREATE TABLE {self.tbl_dst_name} (id INTEGER PRIMARY KEY, title TEXT, "text" TEXT);'
        self.con_dest.execute(sql_d).close()

    def insert_row(self) -> tuple[sqlite.Cursor, list]:
        sql = f'INSERT INTO {self.tbl_src_name} (title, "text") VALUES (?, ?)'
        prms = ['some title', 'some text']

        cursor = self.con_src.execute(sql, prms)
        self.assertEqual(1, cursor.rowcount)
        prms.insert(0, cursor.lastrowid)
        cursor.close()
        return cursor, prms

    def test_insert(self):
        cursor, prms = self.insert_row()

        sql = f'SELECT * FROM {self.tbl_dst_name}'
        r = self.con_dest.execute(sql).fetchone()
        self.assertIsNotNone(r)
        self.assertSequenceEqual(prms, r)

    def test_delete(self):
        cursor, prms = self.insert_row()

        sql = f'DELETE FROM {self.tbl_src_name} WHERE id = ?'
        r = self.con_src.execute(sql, (prms[0],)).fetchone()
        self.assertIsNone(r)

        sql = f'SELECT * FROM {self.tbl_dst_name} WHERE id = ?'
        r = self.con_dest.execute(sql, (prms[0],)).fetchone()
        self.assertIsNone(r)

    def test_update(self):
        cursor, prms = self.insert_row()

        def check_in_dest(prms):
            with self.subTest(title_value=prms[1]):
                # check in dest
                sql = f'SELECT * FROM {self.tbl_dst_name} WHERE id = ?'
                r = self.con_dest.execute(sql, (prms[0],)).fetchone()
                self.assertIsNotNone(r)
                self.assertSequenceEqual(prms, r)

        check_in_dest(prms)

        # update in source
        sql = f'UPDATE {self.tbl_src_name} SET title = ? WHERE id = ?'
        prms[1] += " updated"
        cursor = self.con_src.execute(sql, (prms[1], prms[0]))
        self.assertEqual(1, cursor.rowcount)

        # check in dest again
        check_in_dest(prms)


TRIGGER_TEST_DATA = [117, 'some title -> title', 'some body -> content']


# TODO: Mocks' assertion is not informative cause it evaluates inside sqlite. Consequence is sqlite.OperationalError

class TriggerDriverMock:

    def __init__(self, test_case: TestCase, columns: list) -> None:
        self.test_case = test_case
        self.columns = columns

    def insert(self, rowid, data):
        # data should have fts columns
        test_data = dict(zip(self.columns, TRIGGER_TEST_DATA[1:]))
        self.test_case.assertEqual(117, rowid)
        self.test_case.assertSequenceEqual(test_data, data)


class TriggerBaseMock(TriggerBase):
    trigger_on = 'INSERT'
    trigger_name_suffix = '_after_insert'

    pk_name = 'id'

    def __init__(self, con: sqlite.Connection, table_name: str, column_map: dict, fts_con: sqlite.Connection) -> None:
        super().__init__(con, table_name, column_map, fts_con)
        self.test_case: TestCase = None
        self.fts_driver: TriggerDriverMock = None

    def _trigger_func(self, *args):
        self.test_case.assertSequenceEqual(args, TRIGGER_TEST_DATA)
        super()._trigger_func(*args)

    def get_driver_handler(self):
        handler = super().get_driver_handler()
        self.test_case.assertTrue(callable(handler))
        self.test_case.assertEqual(handler, self.fts_driver.insert)
        return handler

    def trigger_func(self, **kwargs):
        test_data = dict(zip(self.get_trigger_columns(), TRIGGER_TEST_DATA))
        self.test_case.assertDictEqual(test_data, kwargs)
        super().trigger_func(**kwargs)


class TriggerBaseSetupMixin:

    def setUp(self) -> None:
        self.con: sqlite.Connection = sqlite.connect(':memory:')
        self.con.row_factory = sqlite.Row
        self.fts_con: sqlite.Connection = sqlite.connect(':memory:')
        self.fts_con.row_factory = sqlite.Row

        self.col_map = self.get_column_map()
        self.tbl_name = 'test_tbl'

        self.trigger = self.get_trigger()
        self.init_all()

    def get_column_map(self):
        return {
            'id': 'rowid',
            'title': 'title',
            'body': 'content'
        }

    def get_trigger(self):
        trigger = TriggerBaseMock(self.con, self.tbl_name, self.col_map, self.fts_con)
        trigger.fts_driver = self.get_driver()
        trigger.test_case = self
        return trigger

    def get_driver(self):
        return TriggerDriverMock(self, tuple(self.col_map.values())[1:])

    def assert_table(self, con: sqlite.Connection, **where):
        where_str = ' AND '.join((f'{k}=?' for k in where))
        cur = con.execute(
            f'SELECT * FROM sqlite_schema WHERE {where_str}', tuple(where.values())
        )
        self.assertIsNotNone(cur.fetchone())
        cur.close()

    def init_table(self):
        pk_str = 'INTEGER PRIMARY KEY'
        col_def = [
            f'{c} {pk_str}' if i == 0 else f'{c} TEXT' for i, c in enumerate(self.trigger.get_trigger_columns())
        ]
        sql = f'CREATE TABLE {self.tbl_name} ({", ".join(col_def)})'
        self.con.execute(sql).close()
        self.assert_table(self.con, name=self.tbl_name, type='table')

    def init_index(self):
        col_def = [
            c for i, c in enumerate(self.trigger.column_map.values()) if i > 0 and c != self.trigger.pk_name
        ]
        col_def.append("content=''")
        sql = f'CREATE VIRTUAL TABLE {self.trigger.get_fts_table_name()} USING fts5({", ".join(col_def)})'
        self.fts_con.execute(sql).close()
        self.assert_table(self.fts_con, name=self.trigger.get_fts_table_name(), type='table')

    def init_all(self):
        self.init_table()
        self.init_index()

    def tearDown(self) -> None:
        self.con.close()
        self.fts_con.close()


class TestTriggerBase(TriggerBaseSetupMixin, TestCase):

    def test_get_fts_table_name(self):
        self.assertEqual(self.tbl_name + self.trigger.fts_table_name_suffix, self.trigger.get_fts_table_name())

    def test_get_trigger_name(self):
        self.assertEqual(
            self.trigger.table_name + self.trigger.trigger_name_suffix,
            self.trigger.get_trigger_name()
        )

    def test_get_sql_func_name(self):
        self.assertEqual(self.trigger.get_trigger_name() + '_replicate', self.trigger.get_sql_func_name())

    def test_get_fts_rowid(self):
        row = {self.trigger.pk_name: 111, 'ttt': 'ttt_val'}
        self.assertEqual(111, self.trigger.get_fts_rowid(row))

    def test_get_trigger_columns(self):
        self.trigger.column_map = {
            'ttt': 'ttt_',
            self.trigger.pk_name: 'pk_',
            'fff': 'fff_',
        }
        self.assertSequenceEqual([self.trigger.pk_name, 'ttt', 'fff'], self.trigger.get_trigger_columns())

    def test_is_integrated(self):
        self.trigger.register_sql_func();
        self.trigger._create_trigger()
        self.assertIsNone(self.trigger.is_integrated(True))

    def test__create_trigger(self):
        self.trigger._create_trigger()
        trg = self.trigger
        self.assertTrue(trg._is_integrated(
            self.con, name=trg.get_trigger_name(), type='trigger', tbl_name=trg.table_name
        ))

    def test_create(self):
        self.trigger.create()
        self.assert_table(
            self.con, name=self.trigger.get_trigger_name(), type='trigger', tbl_name=self.trigger.table_name
        )

    def test_register_sql_func(self):
        self.trigger.register_sql_func()
        sql = 'SELECT * FROM pragma_function_list WHERE name = ?'
        cur = self.con.execute(sql, (self.trigger.get_sql_func_name(),))
        self.assertIsNotNone(cur.fetchone())
        cur.close()

    def test_drop(self):
        trg = self.trigger
        trg.create()
        trg.drop()
        self.assertTrue(trg._is_sql_function_exist())
        self.assert_table(self.con, name=trg.table_name, type='table')
        self.assert_table(self.fts_con, name=trg.get_fts_table_name(), type='table')

        res = trg._is_integrated(self.con, name=trg.get_trigger_name(), type='trigger', tbl_name=trg.table_name)
        self.assertFalse(res)

    def test_trigger_func(self):
        self.trigger.create()
        sql = f'INSERT INTO {self.trigger.table_name} ({", ".join(self.trigger.get_trigger_columns())}) ' \
              f'VALUES ({", ".join("?" * len(TRIGGER_TEST_DATA))})'
        self.con.execute(sql, TRIGGER_TEST_DATA)

# TODO: these tests (below) do not test the case where one fts5 (index) table has two or more columns,
#  each of which is an index that has a corresponding column in different content tables


class TriggerSetupMixin(TriggerBaseSetupMixin):

    def setUp(self) -> None:
        super().setUp()
        self.trigger.pk_name = 'id'

        self.test_values = [
            [111, 'One hundred one', '111 щось за contents'],
            [115, 'first second third', 'щось за contents'],
            [115, 'once other content', 'щось за інший contents']
        ]
        self.utl = SQLiteFTS5Util(
            self.fts_con, self.trigger.get_fts_table_name(), self.col_map.values(), self.test_values
        )

    def init_all(self):
        self.init_table()
        self.trigger.fts_driver.create_index()

    def get_trigger(self) -> Trigger:
        return InsertTrigger(self.con, self.tbl_name, self.col_map, self.fts_con)


class TestInsertTrigger(TriggerSetupMixin, TestCase):

    def test_insert(self):
        self.trigger.create()
        self.assert_table(
            self.con, name=self.trigger.get_trigger_name(), type='trigger', tbl_name=self.trigger.table_name
        )

        # test complete and partial insert
        ri_fslc = ((0, slice(1, None, None)), (1, slice(2, None, None)), (1, slice(1, 2, None)))
        for i, (tv_idx, fslc) in enumerate(ri_fslc):
            # tv_idx = 0 -> rowid 111 -> two columns (title -> title, body -> content)
            # tv_idx = 1 -> rowid 115 -> one column (body -> content)
            # tv_idx = 2 -> rowid 115 -> one column (title -> title)
            prms = self.test_values[tv_idx][fslc]
            cols = self.trigger.get_trigger_columns()[fslc]
            with self.subTest(step=f'i:{i}:val_row:{tv_idx}:cols:{cols}'):
                # test with dynamic id creation
                sql_fmt = 'INSERT INTO %s (%s) VALUES (%s)'
                cursor = self.con.execute(sql_fmt % (self.tbl_name, ", ".join(cols), ", ".join("?"*len(prms))), prms)
                self.assertEqual(1, cursor.rowcount)
                rowid = cursor.lastrowid
                prms.insert(0, rowid)
                cursor.close()

                # test data from index
                fts_cols = [self.col_map[c] for c in cols]
                fts_tval = []
                for c in fts_cols:
                    fts_tval.extend(self.utl.pretend_v(self.utl[tv_idx, c], rowid, c))
                fts_res = self.utl.res_from_index(rowid)
                self.assertListEqual(self.utl.dicts2hashes(fts_tval), self.utl.dicts2hashes(fts_res))


class TriggerUpdateDeleteSetupMixin(TriggerSetupMixin):

    def get_results(self, test_values: list[dict]):
        tv_res, res = [], []
        for d in test_values:
            rowid = d.pop(self.utl.pk_name)
            res.extend(self.utl.res_from_index(rowid))
            for k, v in d.items():
                tv_res.extend(self.utl.pretend_v(v, rowid, k))
        return tv_res, res

    def pre_init_tables(self):
        self.trigger.create()
        self.assert_table(
            self.con, name=self.trigger.get_trigger_name(), type='trigger', tbl_name=self.trigger.table_name
        )

        #init content table
        idx_tvalues = self.utl.test_values[:2]

        tbl_cols = self.trigger.get_trigger_columns()
        tbl_tvalues = [dict(zip(tbl_cols, d.values())) for d in idx_tvalues]
        sql = f'INSERT INTO {self.tbl_name} ({", ".join(tbl_cols)}) '\
              f'VALUES ({", ".join((f":{f}" for f in tbl_cols))})'
        for d in tbl_tvalues:
            with self.subTest(add_content_rowid=d[self.trigger.pk_name]):
                cursor = self.con.execute(sql, d)
                self.assertEqual(1, cursor.rowcount)

        # test index is empty
        res = []
        for d in idx_tvalues:
            res.extend(self.utl.res_from_index(d['rowid']))

        self.assertFalse(res)

        # direct insertion of test data into fts_table
        self.utl.tval2index(self, stop=2)
        # test appropriate index values
        tv_res, res = self.get_results(idx_tvalues)
        self.assertListEqual(self.utl.dicts2hashes(tv_res), self.utl.dicts2hashes(res))


class TestUpdateTrigger(TriggerUpdateDeleteSetupMixin, TestCase):

    def get_trigger(self) -> InsertTrigger:
        return UpdateTrigger(self.con, self.tbl_name, self.col_map, self.fts_con)

    def test_update(self):
        self.pre_init_tables()

        # change [115][body]  test_values[1, 'content'] in test values
        # [1] [115, 'first second third', 'щось за contents'] -> [115, 'first second third', 'content що став новим']

        new_val = 'content що став новим'
        sql = f'UPDATE {self.tbl_name} SET body=? WHERE id = 115'
        cursor = self.con.execute(sql, (new_val, ))
        self.assertEqual(1, cursor.rowcount)
        cursor.close()

        idx_tvalues = self.utl.test_values[:2]
        idx_tvalues[1]['content'] = new_val
        tv_res, res = self.get_results(idx_tvalues)
        self.assertListEqual(self.utl.dicts2hashes(tv_res), self.utl.dicts2hashes(res))


class TestDeleteTrigger(TriggerUpdateDeleteSetupMixin, TestCase):

    def get_trigger(self) -> Trigger:
        return DeleteTrigger(self.con, self.tbl_name, self.col_map, self.fts_con)

    def test_get_driver_handler(self):
        self.assertEqual(self.trigger.fts_driver.delete_for, self.trigger.get_driver_handler())

    def test_delete(self):
        self.pre_init_tables()

        sql = f'DELETE FROM {self.tbl_name} WHERE id = 115'
        cursor = self.con.execute(sql)
        self.assertEqual(1, cursor.rowcount)
        cursor.close()

        idx_tvalues = self.utl.test_values[:1]
        tv_res, res = self.get_results(idx_tvalues)
        self.assertListEqual(self.utl.dicts2hashes(tv_res), self.utl.dicts2hashes(res))





