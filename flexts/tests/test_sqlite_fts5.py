# IDE: PyCharm
# Project: fts_ua
# Path: ${DIR_PATH}
# File: ${FILE_NAME}
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-19 (y-m-d) 6:30 AM
from typing import Iterable, Union
from unittest import TestCase

import sqlite3 as sqlite

from flexts.sqlite_fts5 import SQLiteFTS5SQLBuilder, SQLiteFTS5


class TestSQLiteFTS5SQLBuilder(TestCase):

    def setUp(self) -> None:
        self.sql_builder = SQLiteFTS5SQLBuilder('test_fts5')

    def test_get_sql_stm_template(self):
        cols = self.sql_builder._cols_template_key
        prms = self.sql_builder._prms_template_key

        template = f"INSERT INTO {self.sql_builder.table_name} (%({cols})s) VALUES (%({prms})s)"
        self.assertEqual(template, self.sql_builder.get_sql_stm_template())

    def test_get_column_list(self):
        # INSERT STATEMENT -> INSERT INTO ft(rowid, a, b, c) VALUES(14, $a, $b, $c);
        # DELETE STATEMENT -> INSERT INTO ft(ft, rowid, a, b, c) VALUES('delete', 14, $a, $b, $c);

        data = {'col_1': 'col_1_value', 'col_2': 'col_2_value'}

        res = self.sql_builder.get_column_list(data)
        self.assertListEqual([*data.keys()], res)

        res = self.sql_builder.get_column_list(tuple(data.keys()))
        self.assertListEqual([*data.keys()], res)

        res = self.sql_builder.get_column_list(data, delete=True)  # for delete
        self.assertListEqual([self.sql_builder.table_name, *data.keys()], res)

        # test data is other iterable
        res = self.sql_builder.get_column_list((c for c in data))
        self.assertListEqual([*data.keys()], res)

    def test_get_param_list(self):
        data = {'col_1': 'col_1_value', 'col_2': 'col_2_value'}

        prms = [f':{c}' for c in data.keys()]

        res = self.sql_builder.get_param_list(data)
        self.assertListEqual(prms, res)

        res = self.sql_builder.get_param_list((c for c in data.keys()))
        self.assertListEqual(prms, res)

        # test data is other iterable
        res = self.sql_builder.get_param_list((c for c in data))
        self.assertListEqual(prms, res)

        prms.insert(0, "'delete'")
        res = self.sql_builder.get_param_list(data, delete=True)  # for delete
        self.assertListEqual(prms, res)

    def test_build(self):
        data = {'col_1': 'col_1_value', 'col_2': 'col_2_value'}
        tn = self.sql_builder.table_name

        stmnt = self.sql_builder.build(data)
        expected = f'INSERT INTO {tn} (col_1, col_2) VALUES (:col_1, :col_2)'
        self.assertEqual(expected, stmnt)

        # test data is other iterable
        stmnt = self.sql_builder.build((c for c in data))
        self.assertEqual(expected, stmnt)

        stmnt = self.sql_builder.build(data, delete=True)
        self.assertEqual(f'INSERT INTO {tn} ({tn}, col_1, col_2) VALUES (\'delete\', :col_1, :col_2)', stmnt)

        stmnt = self.sql_builder.build({}, delete=True)
        self.assertEqual(f'INSERT INTO {tn} ({tn}) VALUES (\'delete-all\')', stmnt)


class SQLiteFTS5Util:

    # index_name: str = 'test_fts'
    # index_columns = ('title', 'text')
    pk_name = 'rowid'
    vocab_suffix = '_v'

    def __init__(self, con: sqlite.Connection, tbl_name, columns, test_values) -> None:
        self.tbl_name = tbl_name
        self.columns = columns
        self.test_values = test_values
        self.con = con

    @property
    def columns(self):
        return [self.pk_name, *self._columns]

    @columns.setter
    def columns(self, columns: Union[Iterable, str]):
        if isinstance(columns, str):
            columns = (columns,)
        self._columns = [c for c in columns if c != self.pk_name]

    @property
    def vocab_name(self) -> str:
        return self.tbl_name + self.vocab_suffix

    @property
    def test_values(self) -> list[dict]:
        cols = self.columns
        return [dict(zip(cols, v)) for v in self._test_values]

    @test_values.setter
    def test_values(self, values: list[Union[tuple, list]]):
        cl = len(self.columns)
        res = []
        for i, v in enumerate(values):
            if len(v) != cl:
                raise ValueError(f'length of item #{i} does not corresponding columns #{cl}')
            res.append(v)
        self._test_values = res

    def __getitem__(self, item):
        if isinstance(item, tuple):
            i, c = item
            if isinstance(c, str):
                c = self.columns.index(c)
            return self._test_values[i][c]
        else:
            return self._test_values[item]

    def tokenize(self, s: str) -> str:
        for t in filter(None, s.strip().split()):
            yield t

    def str_to_dict(self, s: str) -> list[dict]:
        """
            Parse string like
            initstr_data = '''
                term	 doc	col	offset
                111	     111	text	0
                щось	 111	text	1
                за	     111	text	2
                contents 111	text	3
                one	     111	title	0
                hundred	 111	title	1
                one	     111	title	2
            '''
        """
        s = s.strip()
        res = []
        keys = []
        for i, l in enumerate(s.split(sep='\n')):
            llist = self.tokenize(l)
            if i == 0:
                keys = [*llist]
                continue
            d = {keys[j]: v for j, v in enumerate(llist)}
            res.append(d)
        return res

    def pretend_v(self, src: str, rowid, col) -> list[dict]:
        """
        :param src: string of tokens like 'first second third'.
                Each value will put into 'term' with corresponding offset
        :param rowid: value will put into 'doc' column
        :param col: value will put into 'col' column
        :return: Will have as many elements as terms in src
        """
        r = {
            'term': None,
            'doc': str(rowid),
            'col': str(col),
            'offset': None
        }
        res = []
        for i, s in enumerate(self.tokenize(src)):
            res.append(r | {'term': s.lower(), 'offset': str(i)})
        return res

    def res_from_index(self, rowid, column=None) -> list[dict]:
        prms, col_where = [rowid], ''
        if column:
            prms.append(column)
            col_where = f'AND col = ? '

        sql = f'SELECT * FROM {self.vocab_name} WHERE doc = ? {col_where}ORDER BY doc, col, offset'
        cursor = self.con.execute(sql, prms)

        rkeys, res = [], []
        for i, r in enumerate(cursor.fetchall()):
            if i == 0:
                rkeys = r.keys()
            res.append(dict(zip(rkeys, (str(v) for v in r))))
        cursor.close()
        return res

    def d2hashes(self, d: dict) -> list:
        return [hash((hash(k), hash(v))) for k, v in d.items()]

    def dicts2hashes(self, iterator: Iterable[dict], sort=True):
        res = [self.d2hashes(d) for d in iterator]
        if sort:
            res.sort()
        return res

    def tval2index(self, test_case: TestCase, start=None, stop=None, step=None):
        """
            Put test values to index
        """
        slc = slice(start, stop, step)
        cursor = self.con.cursor()
        cols = self.columns
        sql = f"INSERT INTO {self.tbl_name} ({', '.join(cols)}) " \
              f"VALUES ({', '.join((':'+c for c in cols))})"
        values = self.test_values[slc]
        for v in values:
            with test_case.subTest(rowid=v['rowid']):
                cursor.execute(sql, v)
                test_case.assertEqual(1, cursor.rowcount)
        cursor.close()


class TestSQLiteFTS5(TestCase):

    def setUp(self) -> None:
        self.connection: sqlite.Connection = sqlite.connect(':memory:')
        self.index_name = 'test_fts'
        self.index_columns = ('title', 'text')
        self.fts5 = SQLiteFTS5(self.connection, self.index_name, self.index_columns)

        self.test_values = [
            [111, 'One hundred one', '111 щось за contents'],
            [115, 'first second third', 'щось за contents'],
            [115, 'once other content', 'щось за інший contents']
        ]
        self.fts5_utils = SQLiteFTS5Util(self.connection, self.index_name, self.index_columns, self.test_values)

    def tearDown(self) -> None:
        self.connection.close()

    def test_check_table(self):
        self.assertFalse(self.fts5.check_index())

    def test_create_index(self):
        self.assertTrue(self.fts5.create_index())

        # test columns in table
        # [{'cid': 0, 'name': 'title', 'type': '', 'notnull': 0, 'dflt_value': None, 'pk': 0},
        #  {'cid': 1, 'name': 'text', 'type': '', 'notnull': 0, 'dflt_value': None, 'pk': 0}]
        res = [dict(zip(r.keys(), r)) for r in self.connection.execute(f'PRAGMA table_info({self.fts5.index_name})').fetchall()]
        self.assertEqual(len(self.fts5.index_columns), len(res))
        for i in range(len(res)):
            with self.subTest(i=i):
                self.assertIn(res[i]['name'], self.fts5.index_columns)

    def test_drop_index(self):
        self.assertTrue(self.fts5.drop_index())

    def test_check_index_is_broken(self):
        """
        INSERT INTO test_fts5 (rowid, title, text) VALUES (111, 'One hundred one', '111 щось за contents');
        INSERT INTO test_fts5 (rowid, title, text) VALUES (115, 'first second third', 'щось за contents');
        INSERT INTO test_fts5 (rowid, title, text) VALUES (115, 'once other content', 'щось за інший contents');

        :return:
        """
        self.assertTrue(self.fts5.create_index())

        self.fts5_utils.tval2index(self, stop=2)  # like [:2]
        rows = self.fts5.check_index_is_broken()
        self.assertEqual(0, len(rows))

        self.fts5_utils.tval2index(self, -1)  # like [-1:]
        rows = self.fts5.check_index_is_broken()
        self.assertEqual(1, len(rows))
        self.assertEqual(115, rows[0]['rowid'])

        # return details
        rows = self.fts5.check_index_is_broken(return_details=True)
        self.assertEqual(3, len(rows))

        drows = [dict(zip(r.keys(), r)) for r in rows]
        tres = [
            {'terms': 'first,once', 'rowid': 115, 'col': 'title', 'offset': 0, 'cnt': 2},
            {'terms': 'other,second', 'rowid': 115, 'col': 'title', 'offset': 1, 'cnt': 2},
            {'terms': 'content,third', 'rowid': 115, 'col': 'title', 'offset': 2, 'cnt': 2}
        ]
        for i in range(len(tres)):
            with self.subTest(detailed_row_i=i):
                self.assertDictEqual(tres[i], drows[i])

        # TODO: Test SQLiteFTS5TooManyBrokenIndexesError. Now it is hardcoded (more 1000) in source

    def test_delete_for(self):
        self.assertTrue(self.fts5.create_index())
        self.fts5_utils.tval2index(self)

        # index is broken for rowid 115
        rows = self.fts5.check_index_is_broken()
        self.assertEqual(1, len(rows))
        self.assertEqual(115, rows[0]['rowid'])

        self.fts5.delete_for(115)
        # index was deleted for rowid 115
        rows = self.fts5.check_index_is_broken()
        self.assertEqual(0, len(rows))

        # test partial delete for column 'text' [2].
        def get_test_data(column):
            utl = self.fts5_utils
            tres = utl.pretend_v(utl[0, column], 111, column)
            res = utl.res_from_index(111, column)
            return [utl.d2hashes(d) for d in tres], [utl.d2hashes(d) for d in res]

        self.assertListEqual(*get_test_data('text'))

        with self.assertRaises(ValueError):
            self.fts5.delete_for(111, 'text')

        self.fts5.delete_for(111, ('text', ))  # index data was successfully removed for text
        self.assertListEqual(*get_test_data('title'))  # index data still exists for the title
        self.assertListEqual([], get_test_data('text')[1])

    def test_delete_all(self):
        self.assertTrue(self.fts5.create_index())

        self.fts5_utils.tval2index(self)

        sql = f'SELECT term, doc as rowid, col, offset FROM {self.fts5.index_name}_v LIMIT 1'
        cursor = self.connection.execute(sql)
        r = cursor.fetchone()
        self.assertIsNotNone(r)
        self.assertIn(r['rowid'], (rid[0] for rid in self.fts5_utils._test_values))
        cursor.close()

        self.fts5.delete_all()
        cursor = self.connection.execute(sql)
        r = cursor.fetchone()
        self.assertIsNone(r)
        cursor.close()

    def test_insert_index(self):
        self.assertTrue(self.fts5.create_index())

        test_values = self.fts5_utils._test_values[0]  # rowid 111
        rowid = test_values[0]

        # simple test
        data = dict(((k, v) for k, v in zip(self.fts5.index_columns, test_values[1:])))
        self.fts5.insert(rowid, data)

        sql = f'SELECT count(*) FROM {self.fts5.index_name}_v WHERE doc = ?'
        cursor = self.connection.execute(sql, (rowid, ))
        rs = cursor.fetchall()

        # if inserted data is 'One hundred one', '111 щось за contents' then count value must be 7
        self.assertEqual(7, rs[0][0])

    def test_delete_index(self):
        self.assertTrue(self.fts5.create_index())

        self.fts5_utils.tval2index(self)

        test_values = self.fts5_utils._test_values[0]  # rowid 111
        rowid = test_values[0]

        # simple partial delete test
        data = dict(((k, v) for k, v in zip(self.fts5.index_columns[:-1], test_values[1:-1])))
        self.fts5.delete(rowid, data)

        sql = f'SELECT * FROM {self.fts5.index_name}_v WHERE doc = ?'
        cursor = self.connection.execute(sql, (rowid, ))
        r = cursor.fetchone()
        self.assertIsNotNone(r)
        cursor.close()

        self.fts5_utils.tval2index(self)
        # full delete
        data = dict(((k, v) for k, v in zip(self.fts5.index_columns, test_values[1:])))
        self.fts5.delete(rowid, data)

        sql = f'SELECT * FROM {self.fts5.index_name}_v WHERE doc = ?'
        cursor = self.connection.execute(sql, (rowid, ))
        r = cursor.fetchone()
        self.assertIsNone(r)

        cursor.close()

    def test_update_index(self):
        """
            WARN: porter stemmer must be disabled if it used
        """
        # it is hardcoded to values that rowid 111 has

        utl = self.fts5_utils

        self.assertTrue(self.fts5.create_index())
        utl.tval2index(self)

        init = []
        for c in utl.columns[1:]:
            init.extend(utl.pretend_v(utl[0, c], utl[0, 'rowid'], c))

        res = utl.res_from_index(111)

        # test db data == test data
        self.assertListEqual(utl.dicts2hashes(init), utl.dicts2hashes(res))

        # partial update. original 'text' value is '111 щось за contents'
        new_text = 'щось новеньке new value'
        init = utl.pretend_v(new_text, 111, 'text')
        init.extend(utl.pretend_v(utl[0, 'title'], 111, 'title'))
        self.fts5.update(111, {'text': new_text})
        res = utl.res_from_index(111)
        self.assertListEqual(utl.dicts2hashes(init), utl.dicts2hashes(res))
