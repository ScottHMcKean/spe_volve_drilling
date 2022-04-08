from pathlib import Path

import pandas as pd
import plotly.express as px

root_path = Path(r"C:\Users\ibsn\OneDrive - Chevron\Desktop\spe_volve_drilling")
data_path = root_path / "example_data"

file_path = next(data_path.rglob('*.csv'))

# what well are we on
well_name = file_path.name.replace(" depth.csv","").replace("Norway-StatoilHydro-15_$47$_9-","")

drill_df = pd.read_csv(file_path)
drill_df.columns.values

# let's clean up these awful columns
def clean_columns(df):
    df.columns = (df.columns
        .str.replace(r'[^\x00-\x7F]+','', regex=True)
        .str.lower()
        .str.strip()
        .str.replace('/','', regex=False)
        .str.replace('(','', regex=False)
        .str.replace(')','', regex=False)
        .str.replace('.','', regex=False)
        .str.replace(',','', regex=False)
        .str.replace('-','', regex=False)
        .str.replace('%','pct', regex=False)
        .str.replace(' ','_', regex=False)
        )
    return df
    
drill_df.pipe(clean_columns)

drill_df.columns.values

# there is a lot of garbage in this data
import missingno
missingno.matrix(drill_df)

# plots right away with plotly
from plotly.subplots import make_subplots
import plotly.graph_objects as go

fig = make_subplots(rows=2, cols=1)

fig.add_trace(
    go.Scatter(x=drill_df.measured_depth_m, y=drill_df.bit_drill_time_h),
    row=1, col=1
)

fig.add_trace(
    go.Scatter(x=drill_df.measured_depth_m, y=drill_df.mwd_dni_temperature_degc),
    row=2, col=1
)

fig.update_layout(height=800, width=600, title_text=well_name)
fig.show()