import os
import string
from copy import deepcopy
from random import randint, choice
from zipfile import ZipFile
from io import BytesIO

from jinja2 import Template
from lxml import etree

ZIP_NAME_FORMAT = '%03d.zip'
XML_NAME_FORMAT = '%03d.xml'
XML_TEMPLATE = Template('''
<root>
    <var name="id" value="{{ randstr() }}"/>
    <var name="level" value="{{ randint(1, 100) }}"/>
    <objects>
        {% for item in range(randint(1, 10)) -%}
            <object name="{{ randstr() }}"/>
        {% endfor %}
    </objects>
</root>
''')


class ReaderWriter:
    def __init__(self, files_path, num_zip, num_xml, index=0):
        self.files_path = files_path
        self.num_zip = num_zip
        self.num_xml = num_xml

        self.zip_filename = os.path.join(files_path, ZIP_NAME_FORMAT % index)

    def __iter__(self):
        for i in range(self.num_zip):
            yield type(self)(
                self.files_path, self.num_zip, self.num_xml, index=i
            )

    def write(self):
        with open(self.zip_filename, 'wb') as out:
            out.write(self._zip_generate())
            out.flush()

    def _zip_generate(self):
        in_memory = BytesIO()
        with ZipFile(in_memory, mode="w") as zipfh:
            for i in range(self.num_xml):
                zipfh.writestr(XML_NAME_FORMAT % i, self._xml_generate())
        in_memory.seek(0)
        return in_memory.read()

    def _xml_generate(self):
        return XML_TEMPLATE.render(
            randint=randint,
            randstr=lambda: ''.join(
                choice(string.ascii_letters) for _ in range(10)
            )
        )

    def read(self):
        with ZipFile(self.zip_filename, 'r') as zf:
            for i in range(self.num_xml):
                yield self._xml_parse(zf.read(XML_NAME_FORMAT % i).decode())

    def _xml_parse(self, body):
        res = {}
        root = etree.XML(body)
        for element in root:
            if element.tag == 'objects':
                res['objects'] = [
                    e.attrib['name'] for e in element
                ]
            else:
                res[element.attrib['name']] = element.attrib['value']
        return res

    def __repr__(self):
        return "({}, {})".format(self.files_path, self.zip_filename)
