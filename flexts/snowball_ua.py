# IDE: PyCharm
# Project: fts_ua
# Path: examples
# File: snowball_ua.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-14 (y-m-d) 12:38 PM

# Before of all install python3-stemmer Debian package
# create links inside virtual environment
# ....venv/lib/python3.9/site-packages$ ln -s /usr/lib/python3/dist-packages/PyStemmer-2.0.1.egg-info
# ....venv/lib/python3.9/site-packages$ ln -s /usr/lib/python3/dist-packages/Stemmer.cpython-39-x86_64-linux-gnu.so

import sqlitefts
import hunspell
import Stemmer
import snowballstemmer as sbstem

print(Stemmer.algorithms())

# Stemmer.

