from pyspark.conf import SparkConf
from pyspark.sql import SparkSession,DataFrame,functions as F
from pyspark.sql.types import *
from functools import reduce

def union_all(*dfs, fill_by=None):
    try:
        df_union = reduce(DataFrame.unionAll, dfs)
    except:
        cols = {col.name.lower(): (col.dataType, col.name) for df in dfs for col in df.schema.fields}
        dfs = list(dfs)
        for i, df in enumerate(dfs):
            df_cols = [col.lower() for col in df.columns]
            for col, (dataType, name) in cols.items():
                if col not in df_cols:
                    # Add the missing column
                    dfs[i] = dfs[i].withColumn(name, F.lit(fill_by).cast(dataType))
        df_union = reduce(DataFrame.unionByName, dfs)
        
    return df_union

def count_columns(df, *cols, order="count", desc=True):
    list_columns = df.columns
    list_columns.append("count")
    for col in list(cols):
        if col not in list_columns or order not in list_columns:
            raise ValueError("Oops! Column name isn't a valid name. The order parameter must have the name of the count column or one of the columns of the cols parameter..  Try again...")
    if desc:
        win = Window.partitionBy(list(cols))
        df_group = df.withColumn('count', F.count(F.lit(1)).over(win))
    else:
        win = Window.partitionBy(list(cols))
        df_group = df.withColumn('count', F.count(F.lit(1)).over(win)).orderBy(F.col(order))
                  
    return df_group

def avg_column(df, *cols, avg_col):
    for col in list(cols):
        if col not in df.columns or avg_col not in df.columns:
            raise ValueError("Oops! Column name is not a valid name. The column name must be in the dataframe... Try again...")
    win = Window.partitionBy(list(cols))
    df_avg = df.withColumn("avg_" + avg_col, F.mean(F.col(avg_col)).over(win))

    return df_avg

def reduced_dataframe(df_small, df_large, col_small, col_large):
    list_to_broadcast = df_small.select(col_small).rdd.flatMap(lambda x:x).collect()
    df_reduced = df_large.filter(df_large[col_large].isin(list_to_broadcast))

    return df_reduced