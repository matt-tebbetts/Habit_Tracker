import pandas as pd
import os
import glob
from datetime import datetime
import pytz

def scrape_latest_file():
    # get time
    now = datetime.now(pytz.timezone('US/Eastern'))
    now_dt = now.strftime("%Y-%m-%d")
    now_ts = now.strftime("%Y-%m-%d %H:%M:%S")

    # check time
    #if now.minute >= 0:
    #    return
    #else:
    #    print(f"{now_ts}: time to get habit updates...")

    # specify the folder path
    folder_path = 'files/fitnotes_uploads'
    current_fil = 'files/workouts.csv'

    # get the latest file
    files = glob.glob(folder_path + '/*')
    print(f'found {len(files)} files...')
    files.sort(key=os.path.getmtime)
    latest_file = files[-1]

    # compare to current file
    new = pd.read_csv(latest_file)
    cur = pd.read_csv(current_fil)
    if len(new) == len(cur):
        print(f"files are same length: {len(new)}")
    else:
        new['exercise_nbr'] = new.groupby(['Exercise'])['Date'].rank(method='first').astype(int)
        new.to_csv('files/workouts.csv', index=False)
        print("new file. sent to database (workouts.csv, for now)")

scrape_latest_file