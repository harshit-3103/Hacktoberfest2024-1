# Sample DataFrame declaration
data = [("Alice", 34), ("Bob", 45), ("Catherine", 29)]
columns = ["name", "age"]

# For PySpark
df = spark.createDataFrame(data, columns)

# For Pandas
import pandas as pd
df = pd.DataFrame(data, columns=columns)

# Show DataFrame content
df.show()  # PySpark
df.head()  # Pandas

# Select specific columns
df.select("column_name1", "column_name2").show()  # PySpark
df[["column_name1", "column_name2"]].head()      # Pandas

# Filter rows
df.filter(df["column_name"] == "some_value").show()  # PySpark
df[df["column_name"] == "some_value"].head()         # Pandas

# Group by and aggregate
df.groupBy("column_name").agg({"column_name2": "sum"}).show()  # PySpark
df.groupby("column_name")["column_name2"].sum()                # Pandas

# Order by (Sorting)
df.orderBy("column_name", ascending=False).show()  # PySpark
df.sort_values("column_name", ascending=False).head()  # Pandas

# Drop duplicates
df.dropDuplicates(["column_name"]).show()  # PySpark
df.drop_duplicates(subset=["column_name"]).head()  # Pandas

# Drop a column
df.drop("column_name").show()  # PySpark
df.drop(columns=["column_name"]).head()  # Pandas

# Add a new column
from pyspark.sql.functions import lit
df = df.withColumn("new_column", lit("value")).show()  # PySpark
df["new_column"] = "value"  # Pandas

# Join two DataFrames
df1.join(df2, df1["id"] == df2["id"], "inner").show()  # PySpark
pd.merge(df1, df2, on="id", how="inner").head()        # Pandas

# Handle null values
df.na.fill("default_value").show()  # PySpark
df.fillna("default_value").head()   # Pandas
