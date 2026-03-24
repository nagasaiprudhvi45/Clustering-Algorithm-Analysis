from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

def explode_all_arrays(df):
    array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
    for c in array_cols:
        df = df.withColumn(c, explode_outer(col(c)))
    return df

def flatten_structs(df):
    flat_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            for nested in field.dataType.fields:
                flat_cols.append(
                    col(f"{field.name}.{nested.name}").alias(f"{field.name}_{nested.name}")
                )
        else:
            flat_cols.append(col(field.name))
    return df.select(*flat_cols)

def flatten_df_final(df, max_levels=5):
    for _ in range(max_levels):
        df = explode_all_arrays(df)
        df = flatten_structs(df)
    return df

# Example usage
if __name__ == "__main__":
    df = spark.read.option("mergeSchema", "true").json("Files/data/")
    flat_df = flatten_df_final(df)
    display(flat_df)
