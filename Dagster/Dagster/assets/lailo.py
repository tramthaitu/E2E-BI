import os
import pandas as pd
from dagster import asset
from utils.sql import transform_to_sql_server, run_procedure


@asset
def dim_to_staging():
    """
    Đọc DIM Excel và load vào schema DIM (Staging).
    """
    dim_files = {
        "Date": r"C:\Users\Admin\OneDrive - ueh.edu.vn\Documents\Python\DemoLailo\DIM\Date.xlsx",
        "Product": r"C:\Users\Admin\OneDrive - ueh.edu.vn\Documents\Python\DemoLailo\DIM\Products.xlsx",
        "Company": r"C:\Users\Admin\OneDrive - ueh.edu.vn\Documents\Python\DemoLailo\DIM\Company.xlsx"
    }

    for table, path in dim_files.items():
        df = pd.read_excel(path)
        df["SourcePath"] = path
        transform_to_sql_server(
            df=df,
            server="LAPTOP-UVA5Q9QT",
            database_name="LailoStaging",
            schema_name="DIM",
            table_name=table,
            if_exist="replace"
        )
    return "DIM loaded to Staging"

@asset
def fact_to_staging():
    """
    Đọc FACT Lailo từ nhiều folder và load vào schema FACT (Staging).
    """
    folder_cnx = r"C:\Users\Admin\OneDrive - ueh.edu.vn\Documents\Python\DemoLailo\CNX_Fake"
    folder_hst = r"C:\Users\Admin\OneDrive - ueh.edu.vn\Documents\Python\DemoLailo\HST_Fake"

    all_data = []
    for folder in [folder_cnx, folder_hst]:
        for file in os.listdir(folder):
            if file.endswith(".xlsx"):
                path = os.path.join(folder, file)
                df = pd.read_excel(path)
                df["SourceFile"] = path
                all_data.append(df)

    fact_df = pd.concat(all_data, ignore_index=True)
    transform_to_sql_server(
        df=fact_df,
        server="LAPTOP-UVA5Q9QT",
        database_name="LailoStaging",
        schema_name="FACT",
        table_name="Lailo",
        if_exist="replace"
    )
    return f"Fact Lailo loaded ({len(fact_df)} rows)"

@asset
def etl_to_dwh(dim_to_staging, fact_to_staging):
    """
    Chạy ETL stored procedures từ Staging → DWH.
    """
    server = "LAPTOP-UVA5Q9QT"
    database = "LailoDWH"
    procs = [
        "DIM.ETL_Load_DimDate",
        "DIM.ETL_Load_DimProduct",
        "DIM.ETL_Load_DimCompany",
        "FACT.ETL_Load_FactLailo"
    ]
    for p in procs:
        run_procedure(server, database, p)
    return "DWH updated"
