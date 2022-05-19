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