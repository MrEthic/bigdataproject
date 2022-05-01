from src.utils.datalake import LocalDatalakeClient
from src.utils.datalake import LocalDatalakeConfig

#python -m src.scripts.test_datalake_client

print(__name__)

d = LocalDatalakeClient(LocalDatalakeConfig())

#d.put_object({"a": 12, "HHHH": [{0:"zero", 1:"one"}, {"one":1}]}, 'test.json', 'raw', 'test')

