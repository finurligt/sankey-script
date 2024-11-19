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

special_end_cases = [
    {
        "name":"Ghosted",
        "values": [True,None,True,None,False],
    },
    {
        "name":"No response", 
        "values":[True,False,None,None,None],
    }
]

# Start script
df = pd.read_csv(
    filename,
    header=header,
    usecols = active_columns,
    skiprows = skip_rows
)

header = df.columns
end_columns_names = [header[i] for i in end_columns]
print(end_columns_names)

connections = defaultdict(lambda: defaultdict(int)) # Create a defaultdict of defaultdicts to track connections
unactivated = 0

for _, row in df.iterrows():
    reached_end = False

    # Check for special end cases
    end_case = None
    for case in special_end_cases:
        active = True
        row_values = [value for _, value in row.items()] # Unpack the values
        for a, b in zip(case["values"], row_values):
            if a is not None and b is not None and a != b: # If any of the values are not equal
                active = False
                break
        if active: end_case = case["name"]
    
    # If any of the start columns are true we will count the row
    if any(row.iloc[i] for i in start_columns): 
        previous_index = None
        for index, bool in row.items(): # loop through the row
            if bool:
                if previous_index is not None: connections[previous_index][index] += 1
                previous_index = index
                if index in end_columns_names: 
                    reached_end = True
                    break
        if end_case is not None:
            connections[previous_index][end_case] += 1
            reached_end = True
    
        # Warn if no end case and none of the end columns are true
        if not reached_end:
            print(f"Warning: Row {row.name} has no end case and no end column is true.")
            print(row)
    else:
        unactivated += 1

if unactivated > 0: print(f"There are uncounted rows of data: {unactivated}. Have you checked that the start columns are correct?")
pprint(dict(connections),indent=4) # in case you want to print the results

with open("output.txt", "w") as file:
    for key, inner_dict in connections.items():
        for subkey, value in inner_dict.items():
            file.write(f"{key} [{value}] {subkey}\n")

