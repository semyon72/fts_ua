# IDE: PyCharm
# Project: fts_ua
# Path: flexts
# File: parser.py
# Contact: Semyon Mamonov <semyon.mamonov@gmail.com>
# Created by ox23 at 2022-11-27 (y-m-d) 6:34 PM

import io

from lxml import html, etree


class HTMLParser:
    """
    Document
    '<head>Some head data</head> Some head Tail<Script>function() {a < b and c > b}</script> and '\
    '<style>.a {hhhh: #664433}</style> processing too<script type=\'javascript\' Src="/fgdfgs/fgdfg" />'\
    ' like HTML void elements <p> PText <b>bold</b> BTail</p> tail'

    Expected : Some head Tail and  processing too like HTML void elements  PText bold BTail tail

    !!! <head>Some head data</head> processed by special algo. In result tree 'Some head data' is a <p> tag
    """

    skip_tags = {'script': True, 'style': True, 'head': True}
    encoding = 'utf-8'

    def __init__(self) -> None:
        self._parser = etree.HTMLParser(encoding=self.encoding)

    def parse(self, document: str) -> str:
        """
        Expected :             ' Some head Tail and  processing too like HTML void elements  PText bold BTail tail
        Actual   :Some head data Some head Tail and  processing too like HTML void elements  PText bold BTail tail
        Almost good. Looks like -
        <head>Some head data</head> processed by special algo. In result tree 'Some head data' is a <p> tag
        """

        bdoc = io.BytesIO(document.encode(self.encoding))
        iparse = etree.iterparse(bdoc, html=True, encoding=self.encoding)
        for event, el in iparse:
            if el.tag in self.skip_tags:
                el.clear(keep_tail=True)

        return html.HtmlElement(iparse.root).text_content()
