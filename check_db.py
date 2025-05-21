from config import sql_addr
from sqlalchemy import create_engine, text

def check_database():
    engine = create_engine(sql_addr)
    with engine.connect() as conn:
        # Get list of tables
        result = conn.execute(text('SHOW TABLES'))
        tables = [row[0] for row in result]
        print("\nTables in database:")
        for table in tables:
            print(f"\n{table}:")
            # Get table structure
            result = conn.execute(text(f'DESCRIBE `{table}`'))
            for row in result:
                print(f"  {row[0]}: {row[1]}")

if __name__ == '__main__':
    check_database() 