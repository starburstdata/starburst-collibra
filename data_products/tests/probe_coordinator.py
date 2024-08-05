import sys
from trino.dbapi import connect

def initialize_trino_client():
    conn = connect(
        host="localhost",
        port=8080,
        user="probe_user",
    )
    return conn.cursor()

def run_query(cursor):
    sql = "select 1"
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows[0][0] == 1:
            print(0)
        else:
            print(1)
    except:
        print(1)

def main():
    cursor = initialize_trino_client()
    run_query(cursor)

if __name__ == '__main__':
    sys.exit(main())