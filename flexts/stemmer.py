# IDE: PyCharm
# Project: fts_ua
# Path: flexts
# File: stemmer.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-27 (y-m-d) 6:34 PM

import re
from typing import Iterable, Iterator, Callable, Optional

import hunspell

from flexts.parser import HTMLParser


class SimpleTokenizer(Iterable):

    _p = re.compile(r'\w+', re.UNICODE)

    def __init__(self, document: str = None, token_filter: Callable = None) -> None:
        self.document = document
        self.token_filter = token_filter

    @property
    def document(self):
        return self._document

    @document.setter
    def document(self, document):
        self._document = document

    @property
    def token_filter(self) -> Optional[Callable]:
        return self._token_filter

    @token_filter.setter
    def token_filter(self, token_filter: Optional[Callable]):
        if token_filter is not None and not isinstance(token_filter, Callable):
            raise ValueError('token_filter should be either None or callable of one parameter that returns str or None')
        self._token_filter = token_filter

    def __iter__(self) -> Iterator[str]:
        if not self.document:
            raise ValueError('document attribute is empty.')

        for m in self._p.finditer(self.document):
            t = m.group()
            if self.token_filter is not None:
                t = self.token_filter(t)
            if t is not None:
                yield t


class HunspellStemmer(SimpleTokenizer):

    stemmer = hunspell.HunSpell('/usr/share/hunspell/uk_UA.dic', '/usr/share/hunspell/uk_UA.aff')
    min_token_len = 2

    def stems(self, document):
        parsed = HTMLParser().parse(document)
        self.document = parsed
        self.token_filter = str.lower
        for token in self:
            if len(token) <= self.min_token_len:
                continue

            stems = self.stemmer.stem(token)
            if stems:
                for stem in stems:
                    yield stem.decode('utf-8')
            else:
                yield token
