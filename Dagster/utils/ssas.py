import pyodbc
import datetime
import pyadomd

# 1. Lấy tháng mới nhất từ SQL Server (YearMonth)
def get_latest_yearmonth(server: str, database: str):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX([Year Month]) FROM FACT.Lailo;")
        result = cursor.fetchone()[0]
    return result  # VD: '2025-01'

# 2. Hàm gửi XMLA command đến SSAS
def run_xmla(server: str, cmd: str):
    conn_str = f"Data Source={server};"
    with pyadomd.Connection(conn_str) as conn:
        with pyadomd.AdomdCommand(conn, cmd) as command:
            command.ExecuteNonQuery()

# 3. Tạo partition mới (nếu chưa có)
def create_partition(server: str, db: str, table: str, partition: str, sql_query: str):
    cmd = f"""
    {{
      "createOrReplace": {{
        "object": {{
          "database": "{db}",
          "table": "{table}",
          "partition": "{partition}"
        }},
        "partition": {{
          "name": "{partition}",
          "source": {{
            "type": "query",
            "dataSource": "SQL_LailoDWH",
            "query": "{sql_query}"
          }}
        }}
      }}
    }}
    """
    run_xmla(server, cmd)
    print(f"✅ Created partition: {partition}")

# 4. Refresh partition
def refresh_partition(server: str, db: str, table: str, partition: str):
    cmd = f"""
    {{
      "refresh": {{
        "type": "full",
        "objects": [
          {{
            "database": "{db}",
            "table": "{table}",
            "partition": "{partition}"
          }}
        ]
      }}
    }}
    """
    run_xmla(server, cmd)
    print(f"🔄 Refreshed partition: {partition}")

# 5. Pipeline chính (kết hợp tất cả)
def etl_to_ssas(server_sql: str, db_sql: str, server_ssas: str, db_ssas: str, table_ssas: str):
    ym = get_latest_yearmonth(server_sql, db_sql)
    sql_query = f"SELECT * FROM [LailoDWH].[FACT].[Lailo] WHERE [Year Month] = '{ym}'"

    # Tạo partition (nếu chưa có sẽ tự thêm)
    create_partition(server_ssas, db_ssas, table_ssas, ym, sql_query)

    # Refresh partition
    refresh_partition(server_ssas, db_ssas, table_ssas, ym)
