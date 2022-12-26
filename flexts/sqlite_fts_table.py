# IDE: PyCharm
# Project: fts_ua
# Path: flexts
# File: sqlite_fts_table.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-12-03 (y-m-d) 4:27 PM

# It can be difficult to implement in a uniform manner:
#
# 1. Two tables one -> many that need to store in one FTS under rowid of "many".
# At time of insertion into "one" we have no rowid of "many"
# 2. Two tables one -> one (with foreign key field) and ..... as in one. In this case,
# possible use one trigger only for second one (with foreign key field) table and.
# At time of insertion into "one" (with foreign key field) we have all rowid.
#
