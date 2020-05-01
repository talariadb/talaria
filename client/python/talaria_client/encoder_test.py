import unittest

from . import talaria_pb2
from .encoder import Encoder

class TestEncoder(unittest.TestCase):

    def test_encode_batch(self):
        encoder = Encoder()
        event_1 = {
            "key_int": 1,
            "key_bool": True,
            "key_float": 1.0,
            "key_string": "testString",
            "key_string_2": "testString2",
        }
        event_2 = {
            "key_string_dup": "testString",
            "key_int_2": 2,
            "key_string_3": "testString3"
        }
        batch = [event_1, event_2]
        encoder.encode(batch)
        self.assertEqual(11, encoder.index)

    def test_encode_event(self):
        encoder = Encoder()
        event = {
            "key_int": 1,
            "key_bool": True,
            "key_float": 1.0,
            "key_string": "testString",
            "key_string_2": "testString2",
            "key_string_dup": "testString"
        }

        encoder.encode_event(event)
        self.assertEqual(encoder.index, 8)

        return

    def test_encode_value(self):
        encoder = Encoder()
        val_int = 1
        val_bool = False
        val_float = 1.0
        val_string = "testString"
        val_string_2 = "testString2"

        self.assertEqual(talaria_pb2.Value(int64=val_int), encoder.encode_value(val_int))
        self.assertEqual(talaria_pb2.Value(bool=val_bool), encoder.encode_value(val_bool))
        self.assertEqual(talaria_pb2.Value(float64=val_float), encoder.encode_value(val_float))
        self.assertEqual(talaria_pb2.Value(string=1), encoder.encode_value(val_string))
        self.assertEqual(talaria_pb2.Value(string=2), encoder.encode_value(val_string_2))
        self.assertEqual(talaria_pb2.Value(string=1), encoder.encode_value(val_string))

        return

if __name__ == "__main__":
    unittest.main()
