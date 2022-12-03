# IDE: PyCharm
# Project: fts_ua
# Path: ${DIR_PATH}
# File: ${FILE_NAME}
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-19 (y-m-d) 6:30 AM
from unittest import TestCase

from flexts.parser import HTMLParser

DOC_TEST_CONTENT = 'Some head Tail<Script>function() {a < b and c > b}</script> and '\
                   '<style>.a {hhhh: #664433}</style> processing too<script type=\'javascript\' Src="/fgdfgs/fgdfg" />'\
                   ' like HTML void elements <p> PText <b>bold</b> BTail</p> tail'
DOC_TEST_CONTENT_EXPECTED = 'Some head Tail and  processing too like HTML void elements  PText bold BTail tail'


class TestHTMLParser(TestCase):

    def setUp(self) -> None:
        self.parser = HTMLParser()

    def test_parse(self):
        res = self.parser.parse(DOC_TEST_CONTENT)
        self.assertEqual(DOC_TEST_CONTENT_EXPECTED, res)
