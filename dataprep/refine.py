# Module to refine and standardize file
from pathlib import Path
import json

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz, process

from src.data import clean_columns

# specify the file path
raw_path = Path("./raw/")
refined_path = Path("./refined/")
assert raw_path.exists()

# test a single file
file_path = next(raw_path.rglob('*.csv'))

# get the well name
well_name = file_path.name.split(
    "$")[-1].replace(" depth.csv", "").replace("_9-", "").replace(" ", "")

# read the raw dataframe
raw_df = pd.read_csv(file_path)

# clean up the columns and drop the index
raw_df = raw_df.pipe(clean_columns).drop('unnamed:_0', axis=1)

# create a dictionary of metadata to check size and column consistency
# could add more to this including data lake locations, errors, etc.
metadata = {}
metadata[well_name] = {
    'columns': raw_df.columns.tolist(),
    'shape': raw_df.shape
}

# there are 1,775 unique columns across these dataframes, that's brutal
# use this to search columns
raw_df.columns[raw_df.columns.str.contains('rate')]

# so we need to munge using some sort of schema enforcement, with some key columns (say 5)
# enter statistics and probability
columns = {
    'depth':'measured_depth_m',
    'tvd':'hole_depth_tvd_m',
    'lag_tvd':'lag_depth_tvd_m',
    'surface_torque':'average_surface_torque_knm',
    'avg_hookload': 'average_hookload_kkgf',
    'weight_on_bit': 'weight_on_bit_kkgf',
    'avg_rpm':'average_rotary_speed_rpm',
    'rate_of_penetraton':'rate_of_penetration_5ft_avg_mh'
}

# use fuzzy wuzzy to do partial string matching
df_cols = {}
for k,v in columns.items():
    df_cols[k] = process.extractOne(v, raw_df.columns.to_list(),scorer=fuzz.token_set_ratio)[0]

# use our dictionary to rename strings
reverse_df_cols = {v:k for k,v in df_cols.items()}

# establish standard schema
std_df = raw_df[df_cols.values()].rename(columns=reverse_df_cols)

# put together in a loop (that could also be parallized / scaled)
metadata = {}
dfs = []
for file_path in raw_path.rglob("*.csv"):
    try:
        well_name = file_path.name.split(
            "$")[-1].replace(" depth.csv", "").replace("_9-", "").replace(" ", "")
        raw_df = pd.read_csv(file_path)
        raw_df = raw_df.pipe(clean_columns).drop('unnamed:_0', axis=1)

        # use fuzzy wuzzy to do partial string matching
        df_cols = {}
        for k,v in columns.items():
            df_cols[k] = process.extractOne(v, raw_df.columns.to_list(),scorer=fuzz.token_set_ratio)[0]

        # use our dictionary to rename strings
        reverse_df_cols = {v:k for k,v in df_cols.items()}

        # establish standard schema
        std_df = raw_df[df_cols.values()].rename(columns=reverse_df_cols)

        # assign well name
        std_df['well'] = well_name

        # remove duplicate columns and rows and reset index
        std_df = std_df.loc[~std_df.index.duplicated(keep='first'), ~std_df.columns.duplicated(keep='first')].reset_index(drop=True)

        # log metadata
        metadata[well_name] = {
            'columns': raw_df.columns.tolist(),
            'shape': raw_df.shape,
            'column_matches' : df_cols
        }

        # save metadata and standard df to refined
        std_df.to_parquet(refined_path / (well_name + ".parquet"))
        std_df.to_csv(refined_path / (well_name + ".csv"))

        with open(refined_path / (well_name + "_metadata.json"),"w") as f:
            json.dump(metadata, f)
            
    except Exception:
        print(well_name + " import failed")
        continue
        


# initial load completely fails - start working on a schema, getting a complete set of columns
# here is a little script to look at all the columns together
# all_columns = [v['columns'] for k, v in metadata.items()]
# flat_all_cols = [item for sublist in all_columns for item in sublist]
# unique_cols = np.unique(np.asarray(flat_all_cols))


