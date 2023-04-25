#%%

import pandas as pd

# Load the Excel file into a pandas dataframe
df = pd.read_csv('/home/quante/Documents/projects/acclimate-inequality-consumption/material/WORLD_BANK_CLASSIFICATION_2021_20230323_CLASS.csv')

# Create a dictionary for each categorical column
region_dict = {}
income_group_dict = {}

# Iterate over each row in the dataframe
for index, row in df.iterrows():
    # Add the region to the appropriate category dictionary
    if row['Region'] not in region_dict:
        region_dict[row['Region']] = [row['Code']]
    else:
        region_dict[row['Region']].append(row['Code'])

    if row['Income group'] not in income_group_dict:
        income_group_dict[row['Income group']] = [row['Code']]
    else:
        income_group_dict[row['Income group']].append(row['Code'])

# Print the resulting dictionaries
print(income_group_dict)
print(region_dict)
