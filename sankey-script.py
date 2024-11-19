import pandas as pd
from collections import defaultdict
from pprint import pprint

# This part should probably be exported into a "metadata" file
# Everything is 0-indexed

filename = "jobb.csv"
header = 0
active_columns = [10,11,12,13,14]
start_columns = [0]
end_columns = [4]
skip_rows = [1]

# Start script
df = pd.read_csv(
    filename,
    header=header,
    usecols = active_columns,
    skiprows = skip_rows
)

header = df.columns

connections = defaultdict(lambda: defaultdict(int)) # Create a defaultdict of defaultdicts to track connections
unactivated = 0

for _, row in df.iterrows():
    if any(row[i] for i in start_columns): # If any of the start columns are true we will count the row
        previous_index = None
        for index, bool in row.items(): # loop through the row
            if bool: # if the value is true
                if index in end_columns:
                    connections[previous_index][index] += 1
                    break
                else:
                    connections[previous_index][index] += 1
                previous_index = index
    else:
        unactivated += 1

print(f"Unactivated: {unactivated}")
pprint(dict(connections),indent=4)


