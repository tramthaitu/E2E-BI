import os 
import sqlalchemy
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, inspect, text
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def sql_col_type(df: pd.DataFrame) -> dict:
    """
    Sinh dict mapping dtype của Pandas sang SQL Server types.
    """
    dtypedict = {}
    for col, dtype in zip(df.columns, df.dtypes):
        if "object" in str(dtype):
            dtypedict[col] = sqlalchemy.types.NVARCHAR(length=2000)
        elif "datetime" in str(dtype):
            dtypedict[col] = sqlalchemy.types.DateTime()
        elif "float" in str(dtype):
            dtypedict[col] = sqlalchemy.types.DECIMAL(18,3)
        elif "int" in str(dtype):
            dtypedict[col] = sqlalchemy.types.BIGINT()
    return dtypedict


def transform_to_sql_server(df: pd.DataFrame,
                            server: str,
                            database_name: str,
                            schema_name: str,
                            table_name: str,
                            if_exist: str = "append",
                            dict_types: dict = None):
    """
    Load DataFrame vào SQL Server bằng Windows Authentication (Trusted_Connection).
    """
    conn_str = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database_name};"
        "Trusted_Connection=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

    if dict_types is None:
        dict_types = sql_col_type(df)

    with engine.connect() as conn:
        number_of_rows = len(df)
        print(f"==> Loading {number_of_rows} rows vào [{schema_name}].[{table_name}] ...")
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exist,
            index=False,
            dtype=dict_types,
            schema=schema_name
        )
        print("==> Done!")  

def run_procedure(server: str, database: str, procedure: str):
    """
    Gọi procedure trong SQL Server (Windows Authentication).
    """
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(f"EXEC {procedure}")
        conn.commit()
        print(f"==> Executed procedure: {procedure}")


def merge_from_folders(folder_cnx: str, folder_hst: str) -> pd.DataFrame:
    """
    Đọc tất cả file Lailo trong 2 folder CNX và HST, merge thành DataFrame fact.
    """
    all_data = []

    # CNX
    for file in os.listdir(folder_cnx):
        if file.endswith(".xlsx"):
            path = os.path.join(folder_cnx, file)
            df = pd.read_excel(path)
            df["SourceFile"] = path
            all_data.append(df)

    # HST
    for file in os.listdir(folder_hst):
        if file.endswith(".xlsx"):
            path = os.path.join(folder_hst, file)
            df = pd.read_excel(path)
            df["SourceFile"] = path
            all_data.append(df)

    if not all_data:
        raise ValueError("Không tìm thấy file Excel nào trong 2 folder")

    return pd.concat(all_data, ignore_index=True)
