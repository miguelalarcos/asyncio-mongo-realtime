import unittest
#from unittest.mock import MagicMock, patch
from schema import Doc, Schema, public, SetError

class ASchema(Schema):
    schema = {
        "__set_default": public,
        'a': {
            'type': str
        },
        'b': {
            'type': [int]
            }
    }

class XSchema(Schema):
    schema = {
        "__set_default": public,
        'x': {
            'type': int,
            'validation': lambda v: v > -1000
        },
        'A':{
            'type': ASchema
        }
    }  

class XDoc(Doc):
    schema = XSchema


class TestUpdateMethods(unittest.TestCase):

    def test_schema_simple_update(self):
        x = XDoc({'x': 5, 'A': {'a': 'hello :)', 'b': [555]} })
        x.update('x', -500)    
        self.assertDictEqual(x.doc, {'x': -500, 'A': {'a': 'hello :)', 'b': [555]} }) 

    def test_schema_update_nested(self):
        x = XDoc({'x': 5, 'A': {'a': 'hello :)', 'b': [555]} })
        x.update('A.a', 'game over')
        self.assertDictEqual(x.doc, {'x': 5, 'A': {'a': 'game over', 'b': [555]} }) 

    def test_schema_update_array(self):
        x = XDoc({'x': 5, 'A': {'a': 'hello :)', 'b': [555]} })
        x.update('A.b.0', 1)
        self.assertDictEqual(x.doc, {'x': 5, 'A': {'a': 'hello :)', 'b': [1]} })

    def test_schema_update_array_raise(self):
        x = XDoc({'x': 5, 'A': {'a': 'hello :)', 'b': [555]} })
        with self.assertRaises(SetError):
            x.update('A.b', [])

if __name__ == '__main__':
    unittest.main()