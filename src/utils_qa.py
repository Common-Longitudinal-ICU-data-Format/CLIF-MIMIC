import pandera.pandas as pa

all_null_check = pa.Check(
    lambda s: s.isna().all(), 
    element_wise=False, 
    error="Column must contain only null values"
    )