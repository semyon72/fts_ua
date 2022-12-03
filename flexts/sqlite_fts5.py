# IDE: PyCharm
# Project: fts_ua
# Path: flexts
# File: sqlite_fts5.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-27 (y-m-d) 6:35 PM

from typing import Iterable, Mapping, Union, Generator

import sqlite3 as sqlite


class SQLiteFTS5SQLBuilder:

    _cols_template_key = 'columns'
    _prms_template_key = 'params'

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name

    def get_sql_stm_template(self):
        return f"INSERT INTO {self.table_name} (%({self._cols_template_key})s) VALUES (%({self._prms_template_key})s)"

    def get_column_list(self, columns: Iterable, delete=False) -> list:
        if isinstance(columns, Mapping):
            iterable = columns.keys()
        else:
            iterable = columns

        cols = [*iterable]
        if delete:
            cols.insert(0, self.table_name)
        return cols

    def get_param_list(self, columns: Iterable, delete=False) -> list:
        if isinstance(columns, Mapping):
            iterable = columns.keys()
        else:
            iterable = columns

        prms = [f':{c}' for c in iterable]
        if delete:
            cmd = "'delete'"
            if not prms:
                cmd = "'delete-all'"
            prms.insert(0, cmd)
        return prms

    def build(self, columns: Iterable, delete=False) -> str:
        if isinstance(columns, Generator):
            columns = tuple(columns)

        stmnt_prms = {
            self._cols_template_key: ', '.join(self.get_column_list(columns, delete=delete)),
            self._prms_template_key: ', '.join(self.get_param_list(columns, delete=delete)),
        }
        return self.get_sql_stm_template() % stmnt_prms


class SQLiteFTS5TooManyBrokenIndexesError(Exception):
    pass


class SQLiteFTS5:
    pk_name = 'rowid'

    def __init__(self,
                 connection: sqlite.Connection,
                 index_name: str,
                 index_columns: Union[Iterable, str] = 'content',
                 rowfactory=None) -> None:

        self._connection = connection

        if rowfactory is not None:
            self._connection.row_factory = rowfactory
        elif self._connection.row_factory is None:
            self._connection.row_factory = sqlite.Row

        self.index_columns = index_columns

        self.index_name = str(index_name)
        self.sql_builder = SQLiteFTS5SQLBuilder(self.index_name)

    @property
    def index_columns(self):
        return self._index_columns

    @index_columns.setter
    def index_columns(self, index_columns):
        icols = list(index_columns) if isinstance(index_columns, Iterable) else [str(index_columns)]
        try:
            icols.remove(self.pk_name)
        except ValueError:
            pass
        self._index_columns = icols

    def check_index(self) -> bool:
        cursor = self._connection.execute(
            "SELECT * FROM sqlite_schema WHERE type = 'table' and name = ?", (self.index_name, )
        )
        res = False
        if cursor.fetchone():
            res = True
        cursor.close()
        return res

    def create_index(self, extra: dict = None):
        # execute SQL to create contentless fts5 index
        # CREATE VIRTUAL TABLE blog_fts USING fts5(title, text, content='');

        _extra = {'content': ''}
        if extra is not None:
            extra.pop('content', None)
            _extra.update(extra)

        cols = [*self.index_columns, *[f'{p}=\'{v}\'' for p, v in _extra.items()]]
        with self._connection as idx_con:
            if not self.check_index():
                cursor = idx_con.execute(f"CREATE VIRTUAL TABLE {self.index_name} USING fts5 ({', '.join(cols)})")
                assert cursor.rowcount == -1, f'can\'t create fts5 table "{self.index_name}"'
                cursor = cursor.execute(
                    f"CREATE VIRTUAL TABLE IF NOT EXISTS {self.index_name}_v USING fts5vocab ({self.index_name}, instance)"
                )
                cursor.close()
                return cursor.rowcount == -1
            else:
                return True

    def drop_index(self):
        with self._connection as idx_con:
            cursor = idx_con.execute(f"DROP TABLE IF EXISTS {self.index_name}")
            assert cursor.rowcount == -1, f'can\'t drop fts5 table "{self.index_name}"'
            cursor.execute(f"DROP TABLE IF EXISTS {self.index_name}_v")
            cursor.close()
            return cursor.rowcount == -1

    def check_index_is_broken(self, return_details=False) -> list[sqlite.Row]:
        """
        get broken index
        main SQL statement for details
            SELECT group_concat(term) as terms, doc as rowid, col, offset, count(*) as cnt
            FROM test_fts5_v
            GROUP BY doc, col, offset
            HAVING cnt >1
        :return:
        """
        sql_detail = f'SELECT group_concat(term) as terms, doc as rowid, col, offset, count(*) as cnt '\
                     f'FROM {self.index_name}_v GROUP BY doc, col, offset HAVING cnt >1 ORDER BY doc, col, offset'

        sub_sql = f'SELECT doc, count(*) as cnt FROM {self.index_name}_v GROUP BY doc, col, offset HAVING cnt >1'
        sql = f'SELECT doc as rowid, count(*) as error_cnt FROM ( {sub_sql} ) GROUP BY doc'
        sql_cnt = f'SELECT count(*) FROM ({sql})'
        cursor = self._connection.execute(sql_cnt)
        r = cursor.fetchone()
        if r[0] > 1000:
            raise SQLiteFTS5TooManyBrokenIndexesError('number of broken indexes more than 1000')

        s = sql
        if return_details:
            s = sql_detail
        cursor.execute(s)
        res = [r for r in cursor.fetchall()]
        cursor.close()
        return res

    def _check_columns(self, data: Iterable):
        if not set(data).issubset(set(self.index_columns)):
            raise ValueError(f'keys in data is not in {self.__class__.__name__}.index_columns')

    def _get_terms_for(self, rowid, columns: Iterable = None) -> dict:
        """
        Get terms/lexemes from index for certain rowid and column
        it does not include rowid's rowid: value pairs
        only {col: terms, col1: terms, ....}

        TODO: if rowid is string like '115' it will fail. Should be exact int value.
              Fixed by CAST(.... as INTEGER) but need to think which of decides is better
        """

        # SELECT term, doc as rowid, col, offset FROM test_fts5_v WHERE doc = 115 ORDER by rowid, col, offset;
        # term	rowid	col	 offset
        # щось	 115	text	0
        # ....

        prms = {self.pk_name: rowid}
        col_where = ''

        if columns:
            self._check_columns(columns)
            col_where = f'AND col IN ({", ".join(self.sql_builder.get_param_list(columns))}) '
            prms.update(zip(columns, columns))

        sql = f'SELECT term, doc as rowid, col, offset FROM {self.index_name}_v '\
              f'WHERE doc=CAST(:rowid AS INTEGER) {col_where}ORDER BY doc, col, offset'

        data = {}
        cursor = self._connection.execute(sql, prms)
        try:
            for r in cursor.fetchall():
                data.setdefault(r['col'], []).append(r['term'])

            for k, v in data.items():
                if isinstance(v, list):
                    data[k] = ' '.join(v)
        finally:
            cursor.close()
        return data

    def delete_for(self, rowid, columns: Iterable = None):
        with self._connection as con:
            data = self.prepare_data(rowid, self._get_terms_for(rowid, columns))
            sql_del = self.sql_builder.build(data, delete=True)
            cursor = con.execute(sql_del, data)
            assert 1 == cursor.rowcount, f'number of deleted rows is {cursor.rowcount} expected 1'
            cursor.close()

    def delete_all(self):
        """
        data - must be same data for rowid that were inserted before. If data diffs then index will broken.
        """
        with self._connection as idx_con:
            cursor = idx_con.execute(self.sql_builder.build({}, True))
            cursor.close()

    def prepare_data(self, rowid, data: Mapping) -> dict:
        """
            It discards self.pk_name the key in the data in favor of the self.pk_name: rowid pair
        """
        data.pop(self.pk_name, None)
        return {self.pk_name: rowid} | data

    def delete(self, rowid, data: Mapping):
        """
            data - must be same data for rowid that were inserted before. If data diffs then index will broken.
            if data contains rowid key then rowid parameter value will be redefined by data's rowid value
        """
        self._check_columns(data)
        with self._connection as idx_con:
            _data = self.prepare_data(rowid, data)
            cursor = idx_con.execute(self.sql_builder.build(_data, delete=True), _data)
            assert cursor.rowcount == 1, f'cursor.rowcount is {cursor.rowcount} expected 1'
            cursor.close()

    def insert(self, rowid, data: dict):
        """
            rowid should be new.
            data - must be new data for rowid that never has been inserted before.

            If rowid exists and data are literally same - nothing will change.
            If the data are different from the existing ones, the index will be broken.

            if data contains rowid key then rowid parameter value will be redefined by data's rowid value
        """
        self._check_columns(data)
        with self._connection as idx_con:
            _data = self.prepare_data(rowid, data)
            cursor = idx_con.execute(self.sql_builder.build(_data), _data)
            assert cursor.rowcount == 1, f'cursor.rowcount is {cursor.rowcount} expected 1'
            cursor.close()

    def update(self, rowid, data: dict):
        """
            Updates full or partially for certain column
            if data contains rowid key then rowid parameters value will be redefined by data's rowid value
        :param rowid:
        :param data:
        :return:
        """
        self._check_columns(data)
        with self._connection as con:
            old_data = self._get_terms_for(rowid, data)
            if old_data:
                _data = self.prepare_data(rowid, old_data)
                cursor = con.execute(self.sql_builder.build(_data, delete=True), _data)
                assert cursor.rowcount == 1, f'delete step cursor.rowcount is {cursor.rowcount} expected 1'
            _data = self.prepare_data(rowid, data)
            cursor = con.execute(self.sql_builder.build(_data), _data)
            assert cursor.rowcount == 1, f'insert step cursor.rowcount is {cursor.rowcount} expected 1'
            cursor.close()
