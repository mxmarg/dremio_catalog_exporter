Set `DREMIO_ENDPOINT` and `DREMIO_PAT` in `main.py`

To limit scope, set the space names and source folder paths that you want to export in `main.py`:
```
source_selector = [["my-s3-bucket", "folder1"], ["my-glue-catalog", "db1"]]
space_selector = {"DremioSpace1", "UseCase2"}
```

Run `python3 main.py`


Note: A more full-fledged version can also be found here: 
https://github.com/dremio-professional-services/dremio-dbt-exporter/blob/main/dremio_collect_catalog.py
