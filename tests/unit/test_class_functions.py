import unittest

#import modules.DataTransformation
import DataTransformation


class TestDataTransformationMethods(unittest.TestCase):

    def test_extract_search_keyword(self):
        obj = DataTransformation()
        test_string = '&q=ipod&'
        self.assertEqual(obj.extract_search_keyword(test_string), 'ipod')


if __name__ == '__main__':
    unittest.main()
