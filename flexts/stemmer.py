# IDE: PyCharm
# Project: fts_ua
# Path: flexts
# File: stemmer.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-27 (y-m-d) 6:34 PM

import re
import hunspell

from flexts.parser import HTMLParser


class HunspellStemmer:

    stemmer = hunspell.HunSpell('/usr/share/hunspell/uk_UA.dic', '/usr/share/hunspell/uk_UA.aff')
    _p = re.compile(r'\w+', re.UNICODE)
    min_token_len = 2

    def stems(self, document):
        parsed = HTMLParser().parse(document)
        for m in self._p.finditer(parsed):
            token = m.group()
            if len(token) <= self.min_token_len:
                continue

            stems = self.stemmer.stem(token)
            if stems:
                for stem in stems:
                    yield stem.decode('utf-8').lower()
            else:
                yield token.lower()
