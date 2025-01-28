import argparse
from runner.code_runner import CodeRunner


def main(table_names, pg_connection_string):
    runner = CodeRunner(pg_connection_string)
    runner.run(table_names)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run data quality checks for PostgreSQL tables."
    )
    parser.add_argument("--username", required=True, help="Database username")
    parser.add_argument("--password", required=True, help="Database password")
    parser.add_argument("--host", required=True, help="Database host")
    parser.add_argument("--port", required=True, help="Database port")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--table_names", required=True, nargs="+", 
                        help="List of table names (e.g., users trades)")
    args = parser.parse_args()

    pg_connection_string = (
        f"postgresql+psycopg2://{args.username}:{args.password}@"
        f"{args.host}:{args.port}/{args.database}"
    )

    main(args.table_names, pg_connection_string)