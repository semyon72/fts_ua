# IDE: PyCharm
# Project: fts_ua
# Path: fts_sqlite/tests
# File: attach_capability_base.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-12-23 (y-m-d) 11:50 AM
import functools
import sqlite3 as sqlite
from fts_sqlite.blog_sqlite_fts import attach
from fts_sqlite.tests.con_util import ConUtil

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


class AttachCapabilityBase(ConUtil):

    con_tbl_names = ['blog_entry', ]
    fts_tbl_names = ['blog_entry_fts', ]
    fts_v_tbl_names = ['blog_entry_fts_v', ]

    def get_con(self) -> sqlite.Connection:
        raise NotImplementedError

    def get_fts_con(self) -> sqlite.Connection:
        raise NotImplementedError

    def setUp(self) -> None:
        self.con: sqlite.Connection = self.get_con()
        self.fts_con: sqlite.Connection = self.get_fts_con()
        self.attach_as = 'blog'

    def tearDown(self) -> None:
        self.con.close()
        self.fts_con.close()

    def get_schema(self):
        con_schema = {
            self.con_tbl_names[0]: f'CREATE TABLE {self.con_tbl_names[0]} ('
                                   '"id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, '
                                   '"headline" varchar(256) NOT NULL)',
        }

        return con_schema

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
            ],
        }

        return data

    def insert_trigger_func(self, rowid, headline, trigger_action):
        with self.fts_con as con:
            sql = f'INSERT INTO {self.fts_tbl_names[0]} (rowid, headline) VALUES(?, ?)'
            con.execute(sql, (rowid, headline)).close()

    def create_insert_trigger(self, con: sqlite.Connection):
        func_info = ('replicate', -1, self.insert_trigger_func)  # like in definitions of con.create_function

        with con:
            con.create_function(*func_info)
            sql = f'CREATE TRIGGER {self.con_tbl_names[0]}_ai AFTER INSERT ON {self.con_tbl_names[0]} BEGIN'\
                  f' SELECT {func_info[0]}(new.id, new.headline, \'new\'); END;'
            con.execute(sql).close()

    def get_fts_schema(self):
        con_schema = {
            self.fts_tbl_names[0]: f'CREATE VIRTUAL TABLE {self.fts_tbl_names[0]} USING fts5(headline, content='')',
            self.fts_v_tbl_names[0]: f'CREATE VIRTUAL TABLE {self.fts_v_tbl_names[0]} USING '
                                     f'fts5vocab({self.fts_tbl_names[0]}, instance)',
        }
        return con_schema

    def get_data_from_index(self):
        with self.fts_con as con:
            sql = f'SELECT * FROM {self.fts_v_tbl_names[0]} ORDER BY doc, col, offset'
            cur = con.execute(sql)
            try:
                descr = [fd[0] for fd in cur.description]
                res = [dict(zip(descr, r)) for r in cur]
            finally:
                cur.close()
        return res

    def data_to_v(self) -> list[dict]:
        data: list[dict] = self.get_data()[self.con_tbl_names[0]]
        # data - {'id': 111, 'headline': '111 some headline з українською мовою'}
        # v - [{'term': '111', 'doc': 111, 'col': 'headline', 'offset': 0}, ....]
        res = []
        for r in data:
            doc = r['id']
            idx = 0
            for word in r['headline'].lower().split():
                if word:
                    res.append({'term': word.strip(), 'doc': doc, 'col': 'headline', 'offset': idx})
                    idx += 1
        return res

    def indexed_data_test(self, print_v=False):
        ridx = self.get_data_from_index()
        if print_v:
            for r in ridx:
                print('-FTS-INDEX:DATA:', r)

        data_v = self.data_to_v()
        test_hashes = [hash(tuple(hash(kv) for r in d for kv in r.items())) for d in (ridx, data_v)]

        self.assertEqual(*test_hashes)

    def create_schema(self):
        super().create_schema(self.con)
        self.are_tables_exist(self.con)

    def create_fts_schema(self):
        super().create_schema(self.fts_con, self.get_fts_schema)
        self.are_tables_exist(self.fts_con, self.get_fts_schema)

    def insert_data(self):
        super().insert_data(self.con)
        self.check_data(self.con)
