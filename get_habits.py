"""
Habit Bot v1.0
Matthew Tebbetts
2023-03-23

The script gets data from the following sources:
    > Mint.com (from mintapi)
    > Loop Habit Tracker (CSV saved to OneDrive)
    > FitNotes (CSV saved to OneDrive)
    > Scrape CSV from FitNotes export (iPhone to Gmail)

All data goes to a MySQL database hosted on Kamatera

"""

from itertools import islice
import csv
import pandas as pd
import re

# get csv
my_csv = r"C:\Users\matt_\Downloads\Loop Habits CSV 2023-01-14\Checkmarks.csv"

# make dataframe
my_cols = pd.read_csv(my_csv).columns[:-1]
df = pd.DataFrame(columns=my_cols)
print(df)

with open(my_csv, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # skip the header row
    for line in csv_reader:
        txt = ', '.join(line)

        # get date
        cal_dt = txt[0:10]

        # get the values only
        entries = txt.split(", ")
        values = []
        for entry in entries:
            if "value=" in entry:
                value = entry.split("value=")[1].strip("')")
                values.append(value)
        values = list(map(int, values))
        values.insert(0, cal_dt)

        # add to dataframe
        df.loc[len(df)] = values

df.to_csv('files/checkmarks.csv', index=False)
print(df)
print('sent that to files/checkmarks.csv')
print('')

# now make a transposed version
melted = df.melt(id_vars='Date',
                 var_name='Habit',
                 value_vars=my_cols[1:],
                 value_name='Value')

melted.to_csv('files/habits.csv')
print(melted)