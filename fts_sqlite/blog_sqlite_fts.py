# IDE: PyCharm
# Project: fts_ua
# Path: fts_sqlite
# File: blog_sqlite_fts.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-12-05 (y-m-d) 7:09 AM


import sqlite3 as sqlite
from functools import partial
from typing import Callable, Union, Optional
from urllib import parse

from flexts.sqlite_fts5 import SQLiteFTS5
from flexts.sqllitte_backend import InsertTrigger, UpdateTrigger, DeleteTrigger, Trigger, TriggerIntegrityError
from flexts.stemmer import SimpleTokenizer


def get_db_info(con: sqlite.Connection, schema_name: str):
    sql = f"SELECT * FROM pragma_database_list WHERE name=?"
    cursor = con.execute(sql, (schema_name,))
    try:
        r = cursor.fetchone()
    finally:
        cursor.close()
    return r


def is_attached(con: sqlite.Connection, attached_as) -> bool:
    r = get_db_info(con, attached_as)
    return bool(r)


def get_con_uri(con: sqlite.Connection) -> str:
    """
        Get database file information (path)
    """
    r = get_db_info(con, 'main')
    if not r:  # it is not mandatory cause 'main' always exists
        raise ValueError(f'"main" schema is not found. Connection is {con}')

    uri = r[2]
    if not uri:
        raise ValueError(f'"main" schema is not bound to a file for connection "{con}". Not supported.')
    return uri


def attach(con: sqlite.Connection, to: Union[str, sqlite.Connection], as_schema, read_uncommitted: bool = True):
    url = to
    if isinstance(to, sqlite.Connection):
        url = get_con_uri(to)

    if url:
        l, s, q = url.rpartition('?')
        if s and q:
            qs = dict(parse.parse_qsl(q))
            mode = qs.pop('mode', None)
            if mode is None or mode != 'memory':
                qs['mode'] = 'ro'
                url = ''.join((l, s, parse.urlencode(qs, True)))
    else:
        raise ValueError('parameter "to" should be non empty str or not in memory connection')
    with con:
        con.execute(f"ATTACH DATABASE '{url}' AS {as_schema}").close()
        if read_uncommitted is not None:
            con.execute(f'PRAGMA read_uncommitted({read_uncommitted})').close()


class BlogInsertTrigger(InsertTrigger):
    pk_name = 'id'


class BlogUpdateTrigger(UpdateTrigger):
    pk_name = 'id'


class BlogDeleteTrigger(DeleteTrigger):
    pk_name = 'id'


class BlogTriggersBase:
    table_name: str = None
    column_map: dict = None

    trigger_classes = (BlogInsertTrigger, BlogUpdateTrigger, BlogDeleteTrigger)

    def __init__(self, con: sqlite.Connection, fts_con: sqlite.Connection, fts_driver: SQLiteFTS5 = None) -> None:
        self._triggers = []
        fts_drv = fts_driver
        for i, trg_cls in enumerate(self.trigger_classes):
            trg = trg_cls(con, self.table_name, self.column_map, fts_con, fts_drv)
            self._triggers.append(trg)
            # one driver for all index triggers
            if i == 0:
                fts_drv = trg.fts_driver

    @property
    def triggers(self) -> list[Trigger]:
        return self._triggers

    @property
    def fts_table_name(self) -> str:
        res = {t.get_fts_table_name() for t in self.triggers}
        if len(res) != 1:
            raise ValueError(f'fts_table_name is empty or has ambiguous value "{res}"')
        return res.pop()

    @property
    def fts_columns(self) -> list[str]:
        cols = [*self.column_map.values()]
        try:
            cols.remove('rowid')
        except ValueError:
            pass
        return cols


class EntryTriggers(BlogTriggersBase):

    table_name = 'blog_entry'
    column_map = {
        'id': 'rowid',
        'headline': 'headline'
    }


class EntryTextTriggers(BlogTriggersBase):

    table_name = 'blog_entrytext'
    column_map = {
        'id': 'rowid',
        'body_text': 'body_text'
    }


class IndexedDatabase:

    con: sqlite.Connection = None
    con_url = None  # 'file:memorydb_blog?mode=memory&cache=shared'

    fts_con: sqlite.Connection = None
    attach_as = 'blog'  # it is as (schema name) con will attached to fts_con

    def __init__(self, con: sqlite.Connection, fts_con: sqlite.Connection,
                 con_url: str = None, attach_as: str = None) -> None:
        # for each trigger execute is_integrated() with catching TriggerIntegrityError ()
        # 1. Each trigger need to create only once
        # 2. make ensure - trigger - exists -> (will exists after first creation. it persisted in db.sqlite file)
        # 3. each connection was created need to create trigger function

        self.con = con
        self.con_url = con_url
        self.fts_con = fts_con
        self.attach_as = attach_as or self.attach_as

        self.attach_to_content()
        if not self.is_attached():
            raise AssertionError(f'Index has no attached cto content as "{self.attach_as}"')

    def is_attached(self):
        return is_attached(self.fts_con, self.attach_as)

    def attach_to_content(self):
        if not self.is_attached():
            attach(self.fts_con, self.con_url or self.con, self.attach_as)


class BlogFTSIndex(IndexedDatabase):
    """"
        This is container for:

        content tables that are in one (main) database file (original, for instance, that under Django management) and
        second, the index database file that has indexes for corresponding content tables.

        It must keep the relations between content tables and appropriate index table. For now, for simplicity,
        it implemented via ATTACH-ing content database file to index connection.
        But in future

        TODO: It can be implemented like triggering. SQL requests by index will get corresponding id-s from indexes
         and next step will retrieve the real data from content connection. This approach has sense because
         the result set of searched data should not be large or full (more then 100 id-s) at a time.
         Also, this result set can be paginated.

        !!! it is not concerned about ...
    """
    tokenizer_class = SimpleTokenizer
    tokenizer_filter: Optional[Callable] = str.lower.__call__

    def __init__(self, con: sqlite.Connection, fts_con: sqlite.Connection, con_url: str = None,
                 attach_as: str = None) -> None:
        super().__init__(con, fts_con, con_url, attach_as)

        self.entry_triggers = EntryTriggers(con, fts_con)
        self.entry_text_triggers = EntryTextTriggers(con, fts_con)

        for trg in (*self.entry_triggers.triggers, *self.entry_text_triggers.triggers):
            self.resolve_trigger_integrity(trg)

    def resolve_trigger_integrity(self, trigger: Trigger):

        err_resolver_handlers: dict = {
            'content_table': None,
            'trigger': [0, trigger._create_trigger],
            'sql_function': [0, trigger.register_sql_func],
            'fts_table': [0, trigger.fts_driver.create_index]
        }

        res = False
        while not res:
            try:
                trigger.is_integrated(all=True)
            except TriggerIntegrityError as exc:
                handler = err_resolver_handlers[exc.kind]
                err_msg = 'Can not be resolved.{}'
                if handler is None:
                    raise TriggerIntegrityError(exc.kind, exc.name, err_msg.format(''))
                a, h = handler
                if a > 0:
                    raise TriggerIntegrityError(exc.kind, exc.name, err_msg.format(f' "{a}" attempt was done.'))

                handler[0] += 1
                h()
            else:
                res = True

    def limit_sql(self) -> str:
        """
            TODO: Need to be implemented
            page = [1..n]
            per_page = 20
            LIMIT per_page OFFSET (page-1) * per_page - https://www.sqlite.org/lang_select.html#limitoffset

            page = 1
            LIMIT 20 OFFSET 0 -> [1..20] rows

            page = 2
            LIMIT 20 OFFSET 20 -> [21..40] rows

        :return:
        """

        return ''

    @property
    def tokenizer(self):
        if getattr(self, '_tokenizer', None) is None:
            self._tokenizer = self.tokenizer_class()
            func = self.tokenizer_filter
            if hasattr(self.tokenizer_filter, '__self__'):
                # is bound method
                func = type(self).tokenizer_filter

            self._tokenizer.token_filter = func

        return self._tokenizer

    def s_as_match_expr(self, s: str) -> str:
        return s

    def plain2_match_expr(self, s: str, to_prefix=False) -> str:
        """
            plain2_match_expr('The Fat Rats') → "fat" AND "rat"

            It should return query expression like defined in https://www.sqlite.org/fts5.html#full_text_query_syntax
            It shouldn't contain any ' (single quote character) or it must be escaped if ' not inside " (double quotes)
            If inside phrase "term1 term2" need use " then double-quote must be doubled "term1 ""term2"
            Be aware that default tokenizer (unicode61) removes all punctuations.
            This means, index will have not any ' and ". There is why, in general, any matching on ' or " have no sense.

            TODO: Need to think about implementing the same as in PostgreSQL if that has sense
                phrase2_match_expr('The Fat Rats') → "fat" <-> "rat" ???????
                phrase2_match_expr('The Cat and Rats') → "cat" <2> "rat" ???????
                websearch2_match_expr('"fat rat" or cat dog') → "fat" <-> "rat" OR "cat" AND "dog" ???????

        :param s: Plain string
        :param to_prefix: bool
        :return: "fat" AND "rat" if to_prefix is False, otherwise "fat"* AND "rat"*
        """
        self.tokenizer.document = s
        return ' AND '.join((f'"{t}"{"*" if to_prefix else ""}' for t in self.tokenizer))

    def entrytext_match_sql(self, s: str, handler: Callable) -> str:
        """
            It must return id, entry_id, rank. Where id is entrytext.id

            SELECT blog_entrytext_fts5.rowid as id, bet.entry_id, blog_entrytext_fts5.rank
            FROM blog_entrytext_fts5
                INNER JOIN blog.blog_entrytext as bet ON bet.id = blog_entrytext_fts5.rowid
            WHERE blog_entrytext_fts5.body_text MATCH 'ful* tex* sear*') as r

        :param s: input search string (document)
        :param handler: is callable of one parameter for s that should return match expression like 'ful* tex* sear*'
        :return: str is sql
        """
        blog_schema_name = self.attach_as
        match_expr = handler(s)  # like 'ful* tex* sear*'
        trgs = self.entry_text_triggers

        sql = f'SELECT etts.rowid as id, bet.entry_id, etts.rank '\
              f'FROM {trgs.fts_table_name} as etts '\
              f'INNER JOIN {blog_schema_name}.{trgs.table_name} as bet ON bet.id = etts.rowid ' \
              f'WHERE {trgs.fts_table_name} MATCH \'{{{" ".join(trgs.fts_columns) }}} : {match_expr}\' '

        return sql

    def entry_match_sql(self, s, handler: Callable) -> str:
        """
            It must return id, entry_id, rank. Where id is entrytext.id

            SELECT bet.id, blog_entry_fts5.rowid as entry_id, blog_entry_fts5.rank
            FROM blog_entry_fts5
                INNER JOIN blog.blog_entry as be ON be.id = blog_entry_fts5.rowid
                INNER JOIN blog.blog_entrytext as bet ON bet.entry_id = blog_entry_fts5.rowid
            WHERE blog_entry_fts5.headline MATCH 'ful* tex* sear*'

        :param s: input search string (document)
        :param handler: is callable of one parameter for s that should return match expression like 'ful* tex* sear*'
        :return: str is sql
        """
        blog_schema_name = self.attach_as
        match_expr = handler(s)  # like 'ful* tex* sear*'
        trgs = self.entry_triggers
        et_trgs = self.entry_text_triggers

        sql = f'SELECT bet.id, ets.rowid as entry_id, ets.rank '\
              f'FROM {trgs.fts_table_name} as ets '\
              f'INNER JOIN {blog_schema_name}.{trgs.table_name} as be ON be.id = ets.rowid '\
              f'INNER JOIN {blog_schema_name}.{et_trgs.table_name} as bet ON bet.entry_id = ets.rowid '\
              f'WHERE {trgs.fts_table_name} MATCH \'{{{" ".join(trgs.fts_columns) }}} : {match_expr}\' '

        return sql

    def match_sql(self, s: str, handler: Callable) -> str:
        """
        SELECT r.id, group_concat(distinct r.entry_id), sum(r.rank)
        FROM
            (SELECT bet.id, blog_entry_fts5.rowid as entry_id, blog_entry_fts5.rank
            FROM blog_entry_fts5
                INNER JOIN blog.blog_entry as be ON be.id = blog_entry_fts5.rowid
                INNER JOIN blog.blog_entrytext as bet ON bet.entry_id = blog_entry_fts5.rowid
            WHERE blog_entry_fts5.headline MATCH 'ful* tex* sear*'

            UNION ALL

            SELECT blog_entrytext_fts5.rowid as id, bet.entry_id, blog_entrytext_fts5.rank
            FROM blog_entrytext_fts5
                INNER JOIN blog.blog_entrytext as bet ON bet.id = blog_entrytext_fts5.rowid
            WHERE blog_entrytext_fts5.body_text MATCH 'ful* tex* sear*') as r

        :param s: input search string (document)
        :param handler: is callable of one parameter for s that should return match expression like 'ful* tex* sear*'
        :return: str is sql
        """
        sql = 'SELECT r.id, group_concat(distinct r.entry_id) as entry_id, sum(r.rank) '\
              f'FROM ( {self.entry_match_sql(s, handler)} UNION ALL {self.entrytext_match_sql(s, handler)} ) as r '\
              f'GROUP BY r.id ORDER BY sum(r.rank) {self.limit_sql()}'
        return sql

    def match(self, s: str, handler: Union[Callable, str] = 'plain2_match_expr', **handler_args):

        h = handler
        if isinstance(handler, str):
            h = getattr(self, handler, None)
            if h is None:
                raise ValueError(f'self object has no "{handler}" attribute')
        if not isinstance(h, Callable):
            raise ValueError(f'handler "{h}" is not callable')

        sql = self.match_sql(s, partial(h, **handler_args))

        res = []
        cursor = self.fts_con.cursor()
        try:
            cursor.execute(sql)
            for r in cursor.fetchall():
                res.append(r)
        except Exception as exc:
            raise
        finally:
            cursor.close()

        return res
