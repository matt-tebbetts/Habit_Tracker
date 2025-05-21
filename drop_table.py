from config import sql_addr
from sqlalchemy import create_engine, text

def drop_table():
    engine = create_engine(sql_addr)
    with engine.connect() as conn:
        print("Dropping loop_habits table...")
        conn.execute(text('DROP TABLE IF EXISTS loop_habits'))
        conn.commit()
        print("Table dropped successfully")

if __name__ == '__main__':
    drop_table() 