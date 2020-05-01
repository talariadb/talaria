from . import talaria_pb2

class Encoder:

    def __init__(self):
        self.index = 0
        self.dictionary = {}

    def encode(self, batch):
        encoded_events_batch = []
        for event in batch:
            encoded = self.encode_event(event)
            encoded_events_batch.append(encoded)

        return self.encode_dictionary(encoded_events_batch)

    def update_dictionary(self, key):
        ref = self.dictionary.get(key, False)
        if ref is not False:
            return ref

        self.index += 1
        self.dictionary[key] = self.index
        return self.index

    def encode_event(self, event):
        encoded_event = talaria_pb2.Event()
        for k,v in event.items():
            key_ref = self.update_dictionary(k)
            val = self.encode_value(v)
            if val is None:
                continue
            encoded_event.value[key_ref].CopyFrom(val)
        return encoded_event

    def encode_value(self, value):
        if isinstance(value, bool):  # Order matters as bool is considered as int
            return talaria_pb2.Value(bool=value)
        elif isinstance(value, int):
            return talaria_pb2.Value(int64=value)
        elif isinstance(value, float):
            return talaria_pb2.Value(float64=value)
        elif isinstance(value, str):
            key_ref = self.update_dictionary(value)
            return talaria_pb2.Value(string=key_ref)

        return None

    def encode_dictionary(self, encoded_events_batch):
        encoded_batch = talaria_pb2.Batch()
        encoded_batch.events.extend(encoded_events_batch)
        for k, v in self.dictionary.items():
            encoded_batch.strings[v] = bytes(k, 'utf-8')
        return encoded_batch
