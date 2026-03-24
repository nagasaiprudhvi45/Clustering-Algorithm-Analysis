response = requests.get(
    api_url,
    headers=headers,
    params={"collection": "GPS,PERSONS,TELEPHONE_NUMBERS,WORKERS"},
    cert=(CERT_PATH, KEY_PATH)            # ← use lakehouse paths
)

# ── Load directly into Spark ─────────────────────────────
if response.status_code == 200:
    response_json = response.json()
    json_data     = json.dumps([response_json])
    rdd           = spark.sparkContext.parallelize([json_data])
    df            = spark.read.json(rdd)

    df = flatten_df_final_safe(df)

    cols_to_drop = [c for c in df.columns if "." in c]
    df = df.drop(*cols_to_drop)

    # ── Apply Column Order ───────────────────────────────
    df = df.select(column_order)

    display(df)
    df.printSchema()

else:
    print(f"❌ Error: {response.status_code} - {response.text}")

{ 
    "personId": 1243, 
    "title": "Sir", 
    "firstNames": "John", 
    "lastName": "White", 
    "gender": "Male", 
    "dateOfBirth": "1961-10-10T00:00:00", 
    "ageEstimated": false, 
    "dateOfDeath": null, 
    "ethnicity": "Bengali", 
    "maritalStatus": null, 
    "contactAddress": null, 
    "telephoneNumber": [ 
        { 
            "telephoneNumberType": "MOBILE", 
            "phoneNumber": "123456789", 
            "startDate": "2022-06-17T00:00:00", 
            "endDate": null 
        }, 
        { 
            "telephoneNumberType": "HOME", 
            "phoneNumber": "129435366", 
            "startDate": "2022-06-17T00:00:00", 
            "endDate": null 
        } 
    ], 
    "emailAddress": "sirjohnwhite@google.com", 
    "relationship": null, 
    "person": [ 
        { 
            "personId": 1244, 
            "title": "Sir", 
            "firstNames": "Fred", 
            "lastName": "White", 
            "gender": "Male", 
            "dateOfBirth": "2020-06-04T00:00:00", 
            "ageEstimated": false, 
            "dateOfDeath": null, 
            "ethnicity": null, 
            "maritalStatus": null, 
            "contactAddress": "164 Packington Square, LONDON, N1 7UG", 
 
 
 
 
 
Page 9 
Classification – Restricted 
 
 
            "telephoneNumber": [], 
            "emailAddress": null, 
            "relationship": "Step-parent : Step-child", 
            "person": [], 
            "worker": [], 
            "gp": [] 
        } 
    ], 
    "worker": [ 
        { 
            "firstNames": "General", 
            "lastName": "Practitioner", 
            "role": "GP", 
            "roleDescription": "General Practitioner (GP)", 
            "emailAddress": "gp@gplondon.co.uk", 
            "phoneNumber": "2233445566", 
            "organisationName": "Servelec HSC" 
        } 
    ], 
    "gp": [ 
        { 
            "firstNames": "General", 
            "lastName": "Practitioner", 
            "role": "GP", 
            "roleDescription": "General Practitioner (GP)", 
            "emailAddress": "gp@gplondon.co.uk", 
            "phoneNumber": "2233445566", 
            "organisationName": "Servelec HSC" 
        } 
    ] 
} 

-------

from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType, NullType

def is_valid_array(field):
    return (
        isinstance(field.dataType, ArrayType)
        and not isinstance(field.dataType.elementType, NullType)
    )

def is_valid_struct(field):
    return isinstance(field.dataType, StructType)


def flatten_dynamic_safe(df, max_levels=5):
    
    for _ in range(max_levels):

        # 🔹 Step 1: explode only VALID arrays
        for field in df.schema.fields:
            if is_valid_array(field):
                df = df.withColumn(
                    field.name,
                    explode_outer(col(f"`{field.name}`"))
                )

        # 🔹 Step 2: flatten only STRUCTS
        select_expr = []
        struct_found = False

        for field in df.schema.fields:
            field_name = field.name

            if is_valid_struct(field):
                struct_found = True

                for nested in field.dataType.fields:
                    nested_name = nested.name

                    select_expr.append(
                        col(f"`{field_name}`.`{nested_name}`")
                        .alias(f"{field_name}_{nested_name}")
                    )
            else:
                select_expr.append(col(f"`{field_name}`"))

        df = df.select(*select_expr)

        # 🔹 Stop early if no more structs
        if not struct_found:
            break

    return df


--------------------

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

from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType

def explode_all_arrays(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, explode_outer(col(f"`{field.name}`")))
    return df


def flatten_structs_safe(df):
    select_expr = []

    for field in df.schema.fields:
        field_name = field.name

        # 🔹 Handle STRUCT safely
        if isinstance(field.dataType, StructType):
            for nested in field.dataType.fields:
                nested_name = nested.name

                select_expr.append(
                    col(f"`{field_name}`.`{nested_name}`").alias(f"{field_name}_{nested_name}")
                )
        else:
            select_expr.append(col(f"`{field_name}`"))

    return df.select(*select_expr)


def flatten_df_final_safe(df, max_levels=5):
    for _ in range(max_levels):
        df = explode_all_arrays(df)
        df = flatten_structs_safe(df)
    return df


