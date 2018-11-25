#!/usr/bin/env python
import os
import sys
import codecs
import base64
import textract

#UTF8Reader = codecs.getreader('utf8')
#sys.stdin = UTF8Reader(sys.stdin)


#UTF8Writer = codecs.getwriter('utf8')
#sys.stdout = UTF8Writer(sys.stdout)


for line in sys.stdin:
    text = textract.process(os.path.join(os.getcwd(), line))
sys.stdout.write(base64.b64encode(text).decode('utf-8'))
