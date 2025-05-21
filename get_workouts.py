import pandas as pd
import os
import glob
from config import credentials, sql_addr
from sqlalchemy import create_engine
from datetime import datetime
import pytz

# set up connection
engine = create_engine(sql_addr)
now = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')

# find the latest workout file
workouts_folder = 'files/fitnotes_uploads'

# get latest file
files = glob.glob(workouts_folder + '/*')
files.sort(key=os.path.getmtime)
latest_file = files[-1]

# get timestamp and send to sql
df = pd.read_csv(latest_file).query("Date >= '2023-01-01'").reset_index(drop=True)
df['added_ts'] = now
df.to_sql('workout_history', con=engine, if_exists='replace', index_label='set_id')