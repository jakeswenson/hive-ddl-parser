# A Hive DDL Parser for Rust and Python

A hive DDL parser available as a python module.

## Developing

[maturin]: https://github.com/PyO3/maturin

To build and test locally using a virtual environment, run:

```bash
maturin develop
```

then you can run python and import this `hive_ddl_parser` module.

```python
>>> import hive_ddl_parser
>>> hive_ddl_parser.parse_ddl('CREATE TABLE ')
>>> hive_ddl_parser.parse_ddl('CREATE TABLE test(id int);')
{'stored_as': None, 'table_name': 'test', 'partitioned_by': [], 'table_properties': [], 'location': None, 'columns': [('id', 'int')], 'database_name': None, 'row_format': None}
```
