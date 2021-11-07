import unittest
from utils import convert_tickers


class TestModelLogic(unittest.TestCase):
    def test_convert_single_ticker(self):
        ticker = 'ibm.us'
        processed = convert_tickers(ticker)
        self.assertTupleEqual(processed, ('IBM.US', ''))

    def test_convert_list_of_tickers(self):
        tickers = ['ibm.us', 'gs.us', 'dell.us']
        processed = convert_tickers(tickers)
        self.assertTupleEqual(processed, ('IBM.US', 'GS.US', 'DELL.US'))
