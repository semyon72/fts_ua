# IDE: PyCharm
# Project: fts_ua
# Path: fts_sqlite/tests
# File: con_util.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-12-23 (y-m-d) 11:48 AM

import functools
import sqlite3 as sqlite
from typing import Callable
from unittest import TestCase as UnitTestCase

from fts_sqlite.blog_sqlite_fts import is_attached, get_con_uri, get_db_info


# Tests in console
# import sqlite3 as sqlite
# con1 = sqlite.connect('file:memdb?mode=memory&cache=shared', uri=True)
# con1.execute('CREATE TABLE "blog_entry" ("id" integer PRIMARY KEY, "headline" varchar(256))').close()
# con1.execute('INSERT INTO "blog_entry" VALUES (15, "some headlline")').close()
#
# con2 = sqlite.connect(':memory:')
#
# url must be exact same as used in con1. if used file:///memdb then need to used file:///memdb and not file:memdb
# also it touches for query part
#
# con2.execute("ATTACH 'file:memdb?mode=memory&cache=shared' as blog").close()
# [*con2.execute("SELECT * FROM blog.blog_entry")]
#
# >>> Traceback (most recent call last):
# >>>  File "<input>", line 1, in <module>
# >>> sqlite3.OperationalError: database table is locked: blog_entry
#
# con1.execute('COMMIT').close()
# [*con2.execute("SELECT * FROM blog.blog_entry")]
# >>> [(15, 'some headlline')]
#
# detach
# con2.execute("DETACH blog").close()
# check attaches
# [*con2.execute("SELECT * FROM pragma_database_list")]
#
# Next example that avoids uncommited data error
# sqlite3.OperationalError: database table is locked: blog_entry
#
# import sqlite3 as sqlite
# con1 = sqlite.connect('file:memdb?mode=memory&cache=shared', uri=True)
# con1.execute('CREATE TABLE "blog_entry" ("id" integer PRIMARY KEY, "headline" varchar(256))').close()
# con1.execute('INSERT INTO "blog_entry" VALUES (15, "some headlline")').close()
# con2 = sqlite.connect(':memory:')
# con2.execute('PRAGMA read_uncommitted(True)').close()
# con2.execute("ATTACH 'file:memdb?mode=memory&cache=shared' as blog").close()
# [*con2.execute("SELECT * FROM blog.blog_entry")]
# [(15, 'some headlline')]
# con1.in_transaction
# True

class ConUtil(UnitTestCase):

    con_tbl_names = ['blog_entry', 'blog_entrytext']

    def get_schema(self):
        con_schema = {
            self.con_tbl_names[0]: f'CREATE TABLE {self.con_tbl_names[0]} ('
                                   '"id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, '
                                   '"headline" varchar(256) NOT NULL)',
            self.con_tbl_names[1]: f'CREATE TABLE {self.con_tbl_names[1]} ('
                                   '"id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, '
                                   '"body_text" text NOT NULL, '
                                   f'"entry_id" integer NOT NULL REFERENCES {self.con_tbl_names[0]} ("id")'
                                   ' DEFERRABLE INITIALLY DEFERRED)'
        }

        return con_schema

    def create_schema(self, con: sqlite.Connection, get_schema_func: Callable = None):

        if get_schema_func is None:
            get_schema_func = self.get_schema

        con_schema = get_schema_func()

        with con:
            cur = con.cursor()
            try:
                for tbl, sql in con_schema.items():
                    cur.execute(sql)
            finally:
                cur.close()

    def get_data(self) -> dict[str, list[dict]]:
        """
        SQLiteFTS5Util.pretend_v is simple as possible.
        Thus, if compare it to standard unicode61 tokenizer then they are incompatible.
        Therefore, test values should avoid punctuations .... for example "entry_id" - bad value because
        unicode61 -> 'entry' and 'id' tokens but pretend_v -> 'entry_id' token
        :return:
        """

        data = {
            self.con_tbl_names[0]: [
                {'id': 111, 'headline': '111 some headline з українською мовою'},
                {'id': 211, 'headline': '211 second укр мова different in English'},
                {'id': 311, 'headline': '311 third headline without Cyrillic at all'},
            ],
            self.con_tbl_names[1]: [
                {'id': 11111, 'entry_id': 111, 'body_text': '11111 some body text'},
                {'id': 11112, 'entry_id': 111, 'body_text': '11112 second some body text для entryid 111'},
                {'id': 21111, 'entry_id': 211, 'body_text': '21111 щось дуже цікаве with ascii words'},
                {'id': 31111, 'entry_id': 311, 'body_text': '31111 helpful data and translation корисні дані'},
            ]
        }

        return data

    def insert_data(self, con: sqlite.Connection):
        data = self.get_data()
        with con:
            cursor = con.cursor()
            try:
                sql_fmt = 'INSERT INTO {tbl_name} ({cols}) VALUES ({prms})'

                for tbl, rows in data.items():
                    fmt = {'tbl_name': tbl, 'cols': ', '.join(rows[0]), 'prms': ', '.join('?' * len(rows[0]))}
                    sql = sql_fmt.format(**fmt)
                    for r in rows:
                        cursor.execute(sql, tuple(r.values()))
                        with self.subTest(table_row_id=f'{tbl}_{r["id"]}'):
                            self.assertEqual(1, cursor.rowcount)
            finally:
                cursor.close()

    def check_data(self, con: sqlite.Connection, schema_name=''):
        data = self.get_data()
        tbl_names = tuple(data.keys())
        cursor = con.cursor()
        try:
            qual_tbl_name_fmt = f'{schema_name+"." if schema_name else ""}{{}}'
            sql_fmt = 'SELECT count(*) FROM {}'
            for tbl_name in tbl_names:
                rows = data[tbl_name]
                kwargs = {
                    f'is_{qual_tbl_name_fmt.format(tbl_name).replace(".", "__")}': f'has_{len(rows)}_rows'
                }
                with self.subTest(**kwargs):
                    cursor.execute(sql_fmt.format(qual_tbl_name_fmt.format(tbl_name)))
                    r = cursor.fetchone()
                    self.assertEqual(len(rows), r[0])
        finally:
            cursor.close()

    def are_tables_exist(self, con: sqlite.Connection, get_schema_func: Callable = None):

        if get_schema_func is None:
            get_schema_func = self.get_schema

        tbl_names = [*get_schema_func()]

        sql = f'SELECT count(*) FROM sqlite_schema WHERE type=\'table\' AND '\
              f'name IN ({", ".join("?" * len(tbl_names))})'
        cur = con.execute(sql, tbl_names)
        try:
            r = cur.fetchall()
            self.assertTrue(r)
        finally:
            cur.close()

        self.assertEqual(len(tbl_names), r[0][0])

    @staticmethod
    def _get_db_info(con: sqlite.Connection, schema_name: str):
        return get_db_info(con, schema_name)

    def is_attached(self, con: sqlite.Connection, attached_as):
        return is_attached(con, attached_as)

    def get_con_uri(self, con: sqlite.Connection) -> str:
        return get_con_uri(con)

    def are_tables_reachable(self, con: sqlite.Connection, schema_name, tbl_names):
        # get self.blog_index.con and test existence blog_entrytext and blog_entry
        self.assertTrue(self.is_attached(con, schema_name), f'connection {con} has no attachment "{schema_name}"')

        sql = f'SELECT * FROM {schema_name}.sqlite_schema WHERE type=\'table\' AND name=?'
        for tbl_name in tbl_names:
            qual_tbl_name = f'{schema_name}.{tbl_name}'
            with self.subTest(is_table_exists=qual_tbl_name.replace('.', '__')):
                cur = con.execute(sql, (tbl_name,))
                self.assertTrue([*cur], f'Can\'t reach table {qual_tbl_name} in attached database "{schema_name}"')

    def are_content_tables_reachable(self, con: sqlite.Connection, schema_name):
        tbl_names = tuple(self.get_schema().keys())
        self.are_tables_reachable(con, schema_name, tbl_names)


def traceback(con: sqlite.Connection, con_type) -> sqlite.Connection:

    def trace_back(sql, con_type):
        print(f'{con_type}: {sql}')

    if not isinstance(con, sqlite.Connection):
        raise AssertionError(f'parameter con is not sqlite.Connection')

    con.set_trace_callback(functools.partial(trace_back, con_type=con_type))
    return con

