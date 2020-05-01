## Using Python Client

Start ingesting events to TalariaDB with a more user-friendly GRPC client. 

### Installation

```sh
pip install TalariaClient
```

### Quick Usage

```python
from talaria_client import Client

client = Client('IP_ADDRESS:8080')
batch = [
    {
        "event": "testEvent1",
        "time": 1,
    },
    {
        "event": "testEvent2",
        "time": 2
    }
]

client.ingest_batch(batch)
```

### Contributor Instructions

To update the pypi repository:

1. Bump the version in `setup.py`
1. `python3 setup.py sdist bdist_wheel`
1. `python3 -m twine upload --repository pypi dist/*`
