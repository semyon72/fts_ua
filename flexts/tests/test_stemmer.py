# IDE: PyCharm
# Project: fts_ua
# Path: ${DIR_PATH}
# File: ${FILE_NAME}
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-19 (y-m-d) 6:30 AM
from unittest import TestCase

from flexts.stemmer import HunspellStemmer
from flexts.tests.test_parser import DOC_TEST_CONTENT, DOC_TEST_CONTENT_EXPECTED


class TestHunspellStemmer(TestCase):

    def setUp(self) -> None:
        self.stemmer = HunspellStemmer()

    def test_stems(self):
        res = list(self.stemmer.stems(DOC_TEST_CONTENT))
        self.assertListEqual(DOC_TEST_CONTENT_EXPECTED.lower().split(), res)
        ukr_test = 'деякий Українский текст з english словами'
        ukr_test_exp = ['деякий', 'українский', 'текст', 'english', 'слово']
        self.assertListEqual(ukr_test_exp, list(self.stemmer.stems(ukr_test)))
