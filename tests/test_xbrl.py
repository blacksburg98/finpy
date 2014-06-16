import unittest
from finpy.xbrl import XBRL
class TestXBRL(unittest.TestCase):
    def setUp(self):
        pass

    def testmsft(self):
        x = XBRL('msft-20130630.xml')
        
    def testintc(self):
        x = XBRL('intc-20130629.xml')

    def testxom(self):
        x = XBRL('xom-20130630.xml')

    def testaapl(self):
        x = XBRL('aapl-20130629.xml')

if __name__ == '__main__':
    unittest.main()
