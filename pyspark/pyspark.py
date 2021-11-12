from pyspark.conf import SparkConf
from pyspark.sql import SparkSession,DataFrame,functions as F
from pyspark.sql.types import *
from functools import reduce

def union_all(*dfs, fill_by=None):
    try:
        union = reduce(DataFrame.unionAll, dfs)
    except:
        cols = {col.name.lower(): (col.dataType, col.name) for df in dfs for col in df.schema.fields}
        dfs = list(dfs)
        for i, df in enumerate(dfs):
            df_cols = [col.lower() for col in df.columns]
            for col, (dataType, name) in cols.items():
                if col not in df_cols:
                    # Add the missing column
                    dfs[i] = dfs[i].withColumn(name, F.lit(fill_by).cast(dataType))
        union = reduce(DataFrame.unionByName, dfs)
        
    return union

def groupby_count(df, *cols, order="count", desc=True):
    order_columns = list(cols)
    order_columns.extend(['count'])
    if order not in list(order_columns):
        raise ValueError("Oops! Column name isn't a valid name. The order parameter \
                  must have the name of the count \
                  column or one of the columns of the cols parameter..  Try again...")
    if desc:
        group = df.groupby(list(cols)).count().orderBy(F.col(order).desc())
    else:
        group = df.groupby(list(cols)).count().orderBy(F.col(order))
                  
    return group