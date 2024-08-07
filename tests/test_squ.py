import pytest
import os
from squ import SQU

@pytest.fixture(scope="module")
def squ_instance():
    base_path = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(base_path, ".env")
    sql_dir = os.path.join(base_path, "sql")
    return SQU(env_path=env_path, sql_dir=sql_dir, verbose=True)

def drop_first_column(df):
    return df.iloc[:, 1:]

def drop_first_column_polars(df):
    return df[:, 1:]

# Test for qpd with SQL file
@pytest.mark.pandas
def test_qpd_with_file(squ_instance):
    import pandas as pd
    result = squ_instance.qpd("test_query.sql")
    result = drop_first_column(result)
    expected_df = pd.DataFrame({
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Hank'],
        'last_name': ['Doe', 'Doe', 'Smith', 'Brown', 'Johnson', 'Williams', 'Miller', 'Davis', 'Wilson', 'Moore'],
        'age': [28, 32, 24, 45, 35, 29, 31, 40, 27, 38],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'alice.smith@example.com', 'bob.brown@example.com',
                  'charlie.johnson@example.com', 'david.williams@example.com', 'eva.miller@example.com',
                  'frank.davis@example.com', 'grace.wilson@example.com', 'hank.moore@example.com']
    })
    pd.testing.assert_frame_equal(result, expected_df)

# Test for qpd with direct query
@pytest.mark.pandas
def test_qpd_with_query(squ_instance):
    import pandas as pd
    query = "SELECT * FROM test_table;"
    result = squ_instance.qpd(query)
    result = drop_first_column(result)
    expected_df = pd.DataFrame({
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Hank'],
        'last_name': ['Doe', 'Doe', 'Smith', 'Brown', 'Johnson', 'Williams', 'Miller', 'Davis', 'Wilson', 'Moore'],
        'age': [28, 32, 24, 45, 35, 29, 31, 40, 27, 38],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'alice.smith@example.com', 'bob.brown@example.com',
                  'charlie.johnson@example.com', 'david.williams@example.com', 'eva.miller@example.com',
                  'frank.davis@example.com', 'grace.wilson@example.com', 'hank.moore@example.com']
    })
    pd.testing.assert_frame_equal(result, expected_df)

# Test for qpl with SQL file
@pytest.mark.polars
def test_qpl_with_file(squ_instance):
    import polars as pl
    import polars.testing as pt
    result = squ_instance.qpl("test_query.sql")
    result = drop_first_column_polars(result)
    expected_df = pl.DataFrame({
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Hank'],
        'last_name': ['Doe', 'Doe', 'Smith', 'Brown', 'Johnson', 'Williams', 'Miller', 'Davis', 'Wilson', 'Moore'],
        'age': [28, 32, 24, 45, 35, 29, 31, 40, 27, 38],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'alice.smith@example.com', 'bob.brown@example.com',
                  'charlie.johnson@example.com', 'david.williams@example.com', 'eva.miller@example.com',
                  'frank.davis@example.com', 'grace.wilson@example.com', 'hank.moore@example.com']
    })
    pt.assert_frame_equal(result, expected_df)

# Test for qpl with direct query
@pytest.mark.polars
def test_qpl_with_query(squ_instance):
    import polars as pl
    import polars.testing as pt
    query = "SELECT * FROM test_table;"
    result = squ_instance.qpl(query)
    result = drop_first_column_polars(result)
    expected_df = pl.DataFrame({
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Hank'],
        'last_name': ['Doe', 'Doe', 'Smith', 'Brown', 'Johnson', 'Williams', 'Miller', 'Davis', 'Wilson', 'Moore'],
        'age': [28, 32, 24, 45, 35, 29, 31, 40, 27, 38],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'alice.smith@example.com', 'bob.brown@example.com',
                  'charlie.johnson@example.com', 'david.williams@example.com', 'eva.miller@example.com',
                  'frank.davis@example.com', 'grace.wilson@example.com', 'hank.moore@example.com']
    })
    pt.assert_frame_equal(result, expected_df)

# Test for qdd with SQL file
@pytest.mark.dask
def test_qdd_with_file(squ_instance):
    import dask.dataframe as dd
    import pandas as pd
    result = squ_instance.qdd("test_query.sql")
    result = drop_first_column(result)
    expected_df = pd.DataFrame({
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Hank'],
        'last_name': ['Doe', 'Doe', 'Smith', 'Brown', 'Johnson', 'Williams', 'Miller', 'Davis', 'Wilson', 'Moore'],
        'age': [28, 32, 24, 45, 35, 29, 31, 40, 27, 38],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'alice.smith@example.com', 'bob.brown@example.com',
                  'charlie.johnson@example.com', 'david.williams@example.com', 'eva.miller@example.com',
                  'frank.davis@example.com', 'grace.wilson@example.com', 'hank.moore@example.com']
    })
    expected_ddf = dd.from_pandas(expected_df, npartitions=1)
    dd.assert_eq(result, expected_ddf)

# Test for qdd with direct query
@pytest.mark.dask
def test_qdd_with_query(squ_instance):
    import dask.dataframe as dd
    import pandas as pd
    query = "SELECT * FROM test_table;"
    result = squ_instance.qdd(query)
    result = drop_first_column(result)
    expected_df = pd.DataFrame({
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Frank', 'Grace', 'Hank'],
        'last_name': ['Doe', 'Doe', 'Smith', 'Brown', 'Johnson', 'Williams', 'Miller', 'Davis', 'Wilson', 'Moore'],
        'age': [28, 32, 24, 45, 35, 29, 31, 40, 27, 38],
        'email': ['john.doe@example.com', 'jane.doe@example.com', 'alice.smith@example.com', 'bob.brown@example.com',
                  'charlie.johnson@example.com', 'david.williams@example.com', 'eva.miller@example.com',
                  'frank.davis@example.com', 'grace.wilson@example.com', 'hank.moore@example.com']
    })
    expected_ddf = dd.from_pandas(expected_df, npartitions=1)
    dd.assert_eq(result, expected_ddf)

# Test for creating a view with SQL file
@pytest.mark.view
def test_create_view_with_file(squ_instance):
    result = squ_instance.cvw("test_view", "test_view_query.sql")
    assert result is True

# Test for dropping a view
@pytest.mark.view
def test_drop_view(squ_instance):
    result = squ_instance.dvw("test_view")
    assert result is True

# Test for creating a view with direct query
@pytest.mark.view
def test_create_view_with_query(squ_instance):
    view_query = "SELECT first_name, last_name FROM test_table WHERE age > 30;"
    result = squ_instance.cvw("test_view2", view_query)
    assert result is True

@pytest.mark.view
def test_drop_view2(squ_instance):
    result = squ_instance.dvw("test_view2")
    assert result is True
