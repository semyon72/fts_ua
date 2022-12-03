# IDE: PyCharm
# Project: fts_ua
# Path: flexts
# File: sqllitte_backend.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-28 (y-m-d) 2:37 AM


import sqlite3 as sqlite
from typing import Callable, Union

from flexts.sqlite_fts5 import SQLiteFTS5


class TriggerIntegrityError(Exception):

    kinds = ('content_table', 'trigger', 'sql_function', 'fts_table')

    def __init__(self, kind: str, table_name,  *args: object) -> None:
        if kind not in self.kinds:
            raise ValueError(f'unknown kind "{kind}"')

        self.kind = kind

        msg = f'{" ".join(kind.split("_"))} "{table_name}" does not exist'
        if args:
            msg = f'{msg} {args[0]}'

        super().__init__(msg)


class TriggerBase:
    trigger_on = None
    trigger_name_suffix = None
    pk_name = 'rowid'
    fts_table_name_suffix = '_fts5'

    def __init__(self, con: sqlite.Connection, table_name: str, column_map: dict, fts_con: sqlite.Connection) -> None:
        self.column_map = column_map
        self.con = con
        self.table_name = table_name
        self.fts_con = fts_con

    def get_fts_table_name(self) -> str:
        return self.table_name + self.fts_table_name_suffix

    def get_trigger_name(self) -> str:
        return self.table_name + self.trigger_name_suffix

    def get_sql_func_name(self) -> str:
        return self.get_trigger_name() + '_replicate'

    def get_fts_rowid(self, content_row: dict) -> int:
        """
            Concrete implementation that will return effective rowid for fts index.
            If fts index contains the columns from different content tables probably will be need
            run sub select to get effective rowid
        """
        return content_row[self.pk_name]

    def get_trigger_columns(self) -> list:
        cols = [c for c in self.column_map if c != self.pk_name]
        cols.insert(0, self.pk_name)
        return cols

    def _create_trigger(self):
        """
            creates only trigger
        """

        ref_name = 'new'
        if self.trigger_on.lower() == 'delete':
            ref_name = 'old'

        trg_cols = [f"{ref_name}.{c}" for c in self.get_trigger_columns()]
        sql = f'CREATE TRIGGER {self.get_trigger_name()} AFTER {self.trigger_on.upper()} ON {self.table_name} BEGIN'\
              f' SELECT {self.get_sql_func_name()}({", ".join(trg_cols)}); END;'
        self.con.execute(sql).close()

    def create(self):
        """
            creates trigger, registers a sql function and checks for success
        """
        self.register_sql_func()
        self._create_trigger()
        self.is_integrated()

    def register_sql_func(self):
        num_prms = len(self.get_trigger_columns())
        self.con.create_function(self.get_sql_func_name(), num_prms, self._trigger_func)

    @staticmethod
    def _is_integrated(con: sqlite.Connection, **where):
        where_str = ' AND '.join((f'{k}=?' for k in where))
        cur = con.execute(
            f'SELECT * FROM sqlite_schema WHERE {where_str}', tuple(where.values())
        )
        r = cur.fetchone()
        cur.close()
        return r is not None

    def _is_sql_function_exist(self):
        cur = self.con.execute('SELECT * FROM pragma_function_list WHERE name = ?', (self.get_sql_func_name(),))
        r = cur.fetchone()
        cur.close()
        return r is not None

    def is_integrated(self, all=False):
        """
        Check trigger and function existence if 'all' is False. Otherwise all parts will be tested
        :return: raise TriggerIntegrityError
        """
        if all:
            if not self._is_integrated(self.con, name=self.table_name, type='table'):
                TriggerIntegrityError('content_table', self.table_name)
            if not self._is_integrated(self.fts_con, name=self.get_fts_table_name(), type='table'):
                TriggerIntegrityError('fts_table', self.get_fts_table_name())

        if not self._is_integrated(self.con, name=self.get_trigger_name(), type='trigger', tbl_name=self.table_name):
            TriggerIntegrityError('trigger', self.get_trigger_name(), f'for table {self.table_name}')

        if not self._is_sql_function_exist():
            raise TriggerIntegrityError('sql_function', self.get_sql_func_name())

    def drop(self):
        """
            drops only trigger
        :return:
        """
        sql = f'DROP TRIGGER IF EXISTS {self.get_trigger_name()}'
        self.con.execute(sql).close()

        res = self._is_integrated(self.con, name=self.get_trigger_name(), type='trigger', tbl_name=self.table_name)
        assert not res, f'trigger {self.get_trigger_name()} is not deleted'

    def _trigger_func(self, *args):
        """
            Concrete implementation for INSERT, DELETE and UPDATE triggers
            where number of args equal to number defined columns
            if _create_trigger implements, for example, UPDATE trigger along with old + new data
            then it should be redefined
        """
        cols = self.get_trigger_columns()
        assert len(args) == len(cols), f'got not appropriate number of arguments {len(args)} instead {len(cols)}'
        kwargs = dict(zip(cols, args))
        self.trigger_func(**kwargs)

    def get_driver_handler(self) -> Callable[[Union[int, str], dict], None]:
        """
            Concrete implementation for INSERT, DELETE and UPDATE triggers.
            More, see _trigger_func description

            it should returns callable that should be able accept
            rowid - positional argument
            data - dict {column: column_value}
        """
        fts_driver = getattr(self, 'fts_driver')
        handler = getattr(fts_driver, self.trigger_on.lower())
        return handler

    def trigger_func(self, **kwargs):
        """
            Concrete implementation for INSERT, DELETE and UPDATE triggers
            More, see _trigger_func description
        """
        fts_rowid = self.get_fts_rowid(kwargs)
        # data must have keys that mapped into fts columns
        kwargs.pop(self.pk_name, None)
        data = {self.column_map[c]: v for c, v in kwargs.items()}
        self.get_driver_handler()(fts_rowid, data)


class Trigger(TriggerBase):

    fts_driver_class = SQLiteFTS5

    def __init__(self, con: sqlite.Connection, table_name: str, column_map: dict,
                 fts_con: sqlite.Connection, fts_driver: SQLiteFTS5 = None) -> None:
        super().__init__(con, table_name, column_map, fts_con)

        driver = fts_driver
        if fts_driver is None:
            driver = self.fts_driver_class(fts_con, self.get_fts_table_name(), self.column_map.values())
        self.fts_driver = driver


class InsertTrigger(Trigger):
    trigger_on = 'INSERT'
    trigger_name_suffix = '_ai'


class UpdateTrigger(Trigger):
    trigger_on = 'UPDATE'
    trigger_name_suffix = '_au'


class DeleteTrigger(Trigger):
    trigger_on = 'DELETE'
    trigger_name_suffix = '_ad'

    def get_driver_handler(self) -> Callable[[Union[int, str], dict], None]:
        return self.fts_driver.delete_for
