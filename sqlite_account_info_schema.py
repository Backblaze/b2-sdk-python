import tempfile

import operator

from sadisplay import describe, render
from sqlalchemy import create_engine, MetaData

from b2sdk.account_info.sqlite_account_info import SqliteAccountInfo


def main():
    with tempfile.NamedTemporaryFile() as fp:
        sqlite_db_name = fp.name
        SqliteAccountInfo(sqlite_db_name)
        engine = create_engine('sqlite:///' + sqlite_db_name)

        meta = MetaData()

        meta.reflect(bind=engine)

        tables = set(meta.tables.keys())

        desc = describe(
            map(lambda x: operator.getitem(meta.tables, x), sorted(tables)))
        print(getattr(render, 'dot')(desc).encode('utf-8'))


if __name__ == '__main__':
    main()
