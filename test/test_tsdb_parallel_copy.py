import os
import tempfile
import uuid

import pandas as pd
from sqlalchemy import create_engine, text


from tsdb_parallel_copy import tsdb_parallel_copy


def test_pd_to_timescale(timescale_endpoint, cleanup):
    table_name = f"dummy_{uuid.uuid1().hex}"
    file_name = os.path.join(tempfile.gettempdir(), table_name+".csv")
    ref = pd.DataFrame(
        [
            dict(
                time=pd.to_datetime((1689260352 + 100 * i) * 1e9), a=float(i), b=int(i)
            )
            for i in range(64)
        ]
    )
    engine = create_engine(timescale_endpoint)

    def cleanup_table():
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE {table_name};"))
        engine.dispose()

    cleanup.append(cleanup_table)

    ref.to_csv(file_name, index=False)
    tsdb_parallel_copy(file_name, timescale_endpoint, table_name)
    new = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    assert ref.equals(new)