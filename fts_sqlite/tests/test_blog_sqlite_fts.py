# IDE: PyCharm
# Project: fts_ua
# Path: ${DIR_PATH}
# File: ${FILE_NAME}
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-12-06 (y-m-d) 1:14 PM

import os
import sqlite3 as sqlite

from flexts.tests.test_sqlite_fts5 import SQLiteFTS5Util
from fts_sqlite.blog_sqlite_fts import BlogFTSIndex, attach
import fts_sqlite.tests.con_util as conutil


class BlogFTSIndexInFileSetup(conutil.ConUtil):

    def setUp(self) -> None:

        self.work_dir = '/home/ox23/PycharmProjects/fts_ua/.work/'
        self.con_db_file = f'{self.work_dir}blog_content.sqlite3'

        self.reset_db(self.con_db_file)
        self.con_url = f'file:{self.con_db_file}'
        self.con: sqlite.Connection = conutil.traceback(sqlite.connect(self.con_url, timeout=.1), 'CONTENT')

        self.create_schema(self.con)

        self.fts_con_db_file = f'{self.work_dir}blog_fts_index.sqlite3'
        self.reset_db(self.fts_con_db_file)
        fts_con_url = f'file:{self.fts_con_db_file}'
        self.fts_con: sqlite.Connection = conutil.traceback(sqlite.connect(fts_con_url, timeout=.1), 'FTS_INDEX')

        self.attach_as = 'blog'

        self.blog_index = BlogFTSIndex(self.con, self.fts_con, self.con_url, self.attach_as)

        self.are_content_tables_reachable(self.fts_con, self.attach_as)

    @staticmethod
    def reset_db(file):
        if os.path.isfile(file):
            os.remove(file)

    def tearDown(self) -> None:
        for db_file, con in ((self.fts_con_db_file, self.fts_con),(self.con_db_file, self.con)):
            con.close()
            # self.reset_db(db_file)


class BlogFTSIndexInMemorySetup(conutil.ConUtil):
    '''
        ATTENTION: If use it as TestBlogFTSIndex(BlogFTSIndexInMemorySetup) then test will hangs up
        as described in TestInMemoryContentAttachedCapability (skipped)
    '''

    def setUp(self) -> None:

        self.con_url = f'file:memorydb_blog_content?mode=memory&cache=shared'
        self.con: sqlite.Connection = conutil.traceback(sqlite.connect(self.con_url, timeout=.1, uri=True), 'CONTENT')
        self.create_schema(self.con)

        fts_con_url = f'file:memorydb_blog_fts_index?mode=memory&cache=shared'
        self.fts_con: sqlite.Connection = conutil.traceback(sqlite.connect(fts_con_url, timeout=.1, uri=True), 'FTS_INDEX')
        self.attach_as = 'blog'

        self.blog_index = BlogFTSIndex(self.con, self.fts_con, self.con_url, self.attach_as)

        self.are_content_tables_reachable(self.fts_con, self.attach_as)

    def tearDown(self) -> None:
        self.fts_con.close()
        self.con.close()


class TestBlogFTSIndex(BlogFTSIndexInFileSetup):

    def test_insert(self):
        self.insert_data(self.con)
        self.check_data(self.con)
        self.check_data(self.fts_con, self.attach_as)

        tbl_trg = {
            "blog_entry": self.blog_index.entry_triggers,
            "blog_entrytext": self.blog_index.entry_text_triggers
        }

        for tbl, rows in self.get_data().items():

            utl = SQLiteFTS5Util(
                self.blog_index.fts_con,
                tbl_trg[tbl].fts_table_name,
                tbl_trg[tbl].column_map.values(),
                tuple(tuple(v for k, v in d.items() if k != 'entry_id') for d in rows)
                # trick, 'entry_id' exists only in one table
            )

            init, res = [], []
            for i in range(len(utl._test_values)):
                rowid = utl[i, 'rowid']
                res.extend(utl.res_from_index(rowid))
                for c in utl.columns[1:]:
                    init.extend(utl.pretend_v(utl[i, c], rowid, c))

            with self.subTest(test_table=tbl):
                # test db data == test data
                self.assertListEqual(utl.dicts2hashes(init), utl.dicts2hashes(res))

    def test_s_as_match_expr(self):
        self.insert_data(self.con)
        self.check_data(self.con)
        self.check_data(self.fts_con, self.attach_as)

        self.assertEqual('one* two*', self.blog_index.s_as_match_expr('one* two*'))

    def test_plain2_match_expr(self):
        self.assertEqual('"one" AND "two"', self.blog_index.plain2_match_expr('one, Two'))
        self.assertEqual('"one"* AND "two"*', self.blog_index.plain2_match_expr('one, Two', True))

    def get_exp_entrytext_match_sql(self):
        b_schema = self.blog_index.attach_as
        tsql = 'SELECT etts.rowid as id, bet.entry_id, etts.rank '\
               f'FROM blog_entrytext_fts5 as etts INNER JOIN {b_schema}.blog_entrytext as bet ON bet.id = etts.rowid '\
               'WHERE blog_entrytext_fts5 MATCH \'{body_text} : "one" AND "two"\' '
        return tsql

    def test_entrytext_match_sql(self):
        exp_sql = self.get_exp_entrytext_match_sql()
        sql = self.blog_index.entrytext_match_sql('One tWo', self.blog_index.plain2_match_expr)
        self.assertEqual(exp_sql, sql)

    def get_exp_entry_match_sql(self):
        b_schema = self.blog_index.attach_as
        tsql = 'SELECT bet.id, ets.rowid as entry_id, ets.rank ' \
               'FROM blog_entry_fts5 as ets ' \
               f'INNER JOIN {b_schema}.blog_entry as be ON be.id = ets.rowid ' \
               f'INNER JOIN {b_schema}.blog_entrytext as bet ON bet.entry_id = ets.rowid '\
               'WHERE blog_entry_fts5 MATCH \'{headline} : "one" AND "two"\' '
        return tsql

    def test_entry_match_sql(self):
        exp_sql = self.get_exp_entry_match_sql()
        sql = self.blog_index.entry_match_sql('One tWo', self.blog_index.plain2_match_expr)
        self.assertEqual(exp_sql, sql)

    def test_match_sql(self):
        exp_sql = 'SELECT r.id, group_concat(distinct r.entry_id) as entry_id, sum(r.rank) '\
                  f'FROM ( {self.get_exp_entry_match_sql()} UNION ALL {self.get_exp_entrytext_match_sql()} ) as r '\
                  'GROUP BY r.id ORDER BY sum(r.rank) '
        sql = self.blog_index.match_sql('One tWo', self.blog_index.plain2_match_expr)
        self.assertEqual(exp_sql, sql)

    def test_match(self):
        self.insert_data(self.con)
        self.check_data(self.con)
        self.check_data(self.fts_con)

        # test values
        # self.con_tbl_names[0]: [
        #     {'id': 111, 'headline': '111 some headline з українською мовою'},
        #     {'id': 211, 'headline': '211 second укр мова different in English'},
        #     {'id': 311, 'headline': '311 third headline without Cyrillic at all'},
        # ],
        # self.con_tbl_names[1]: [
        #     {'id': 11111, 'entry_id': 111, 'body_text': '11111 some body text'},
        #     {'id': 11112, 'entry_id': 111, 'body_text': '11112 second some body text для entryid 111'},
        #     {'id': 21111, 'entry_id': 211, 'body_text': '21111 щось дуже цікаве with ascii words'},
        #     {'id': 31111, 'entry_id': 311, 'body_text': '31111 helpful data and translation корисні дані'},
        # ]
        # result fields
        # 'id, entry_id, sum(r.rank) as rank'

        res = self.blog_index.match('цікаве')  # 21111 - цікаве
        self.assertTrue(res)
        self.assertIsInstance(res, list)
        self.assertEqual(1, len(res))
        self.assertSequenceEqual([21111, '211'], res[0][:-1])

        res = self.blog_index.match('w*', 's_as_match_expr')  # 21111 - with .... words & 31111 - without
        self.assertTrue(res)
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))
        self.assertSequenceEqual([21111, '211'], res[0][:-1])
        self.assertSequenceEqual([31111, '311'], res[1][:-1])

        res = self.blog_index.match('nothing')  # nothing found
        self.assertFalse(res)
        self.assertIsInstance(res, list)

    def test_delete(self):
        self.insert_data(self.con)
        self.check_data(self.con)
        self.check_data(self.fts_con)

        # will remove
        # {'id': 31111, 'entry_id': 311, 'body_text': '31111 helpful data and translation корисні дані'},

        match_word = 'helpful'

        res = self.blog_index.match(match_word)
        self.assertTrue(res)
        self.assertIsInstance(res, list)
        self.assertEqual(1, len(res))
        self.assertSequenceEqual([31111, '311'], res[0][:-1])

        with self.con as con:
            sql_fmt = f'DELETE FROM {self.con_tbl_names[1]} WHERE id = 31111'
            cur = con.execute(sql_fmt)
            try:
                self.assertEqual(1, cur.rowcount)
            finally:
                cur.close()

        res = self.blog_index.match(match_word)  # nothing found
        self.assertFalse(res)
        self.assertIsInstance(res, list)

    def test_update(self):
        self.insert_data(self.con)
        self.check_data(self.con)
        self.check_data(self.fts_con)

        # will update
        #     {'id': 211, 'headline': '211 second укр мова different in English'},
        # to
        #     {'id': 211, 'headline': '211 some was added to - second укр мова in English'},

        match_word = 'some'

        res = self.blog_index.match(match_word)
        self.assertTrue(res)
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))
        self.assertSequenceEqual([11111, '111'], res[0][:-1])
        self.assertSequenceEqual([11112, '111'], res[1][:-1])

        # testing the 'different' to test an index consistency after updated
        res = self.blog_index.match('different')
        self.assertTrue(res)
        self.assertIsInstance(res, list)
        self.assertEqual(1, len(res))
        self.assertSequenceEqual([21111, '211'], res[0][:-1])

        with self.con as con:
            sql_fmt = f'UPDATE {self.con_tbl_names[0]} SET headline = ? WHERE id = 211'
            cur = con.execute(sql_fmt, ('211 some was added to - second укр мова in English', ))
            try:
                self.assertEqual(1, cur.rowcount)
            finally:
                cur.close()

        res = self.blog_index.match(match_word)  # 11111, 11112, 21111
        self.assertTrue(res)
        self.assertIsInstance(res, list)
        self.assertEqual(3, len(res))
        self.assertSequenceEqual([11111, '111'], res[0][:-1])
        self.assertSequenceEqual([11112, '111'], res[1][:-1])
        self.assertSequenceEqual([21111, '211'], res[2][:-1])

        # Re-testing the 'different' to test an index consistency after updated
        res = self.blog_index.match('different')
        self.assertFalse(res)
        self.assertIsInstance(res, list)
