# Make produced files
# Module to refine and standardize file
from pathlib import Path
import json

import numpy as np
import pandas as pd

# specify the file path
refined_path = Path("./refined/")
produced_path = Path("./produced/")

# read .parquet files (what real data people use), upscale
dfs = []
for filepath in refined_path.rglob('*.parquet'):

    ref_df = pd.read_parquet(filepath)
    
    # resample to every 10 m
    ref_df["rounded_depth"] = ref_df.depth.round(0)

    # groupby and grab mean for upsampling to every meter using average
    prod_df = ref_df.select_dtypes("number").groupby("rounded_depth").mean()
    prod_df['well'] = ref_df.well[0]

    # drop nans - we will need to be more sophisticated here 
    # but this gives us clean data to start
    prod_df = prod_df.dropna()

    # we will do more here, but this is a good start, so combine in memory 
    dfs.append(prod_df)
    
# write out combined clean data to produced
combined = pd.concat(dfs)
combined.to_parquet('volve_wells.parquet')
combined.to_csv('volve_wells.csv')