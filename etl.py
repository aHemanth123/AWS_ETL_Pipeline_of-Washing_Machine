import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
import pyspark.sql.functions as F

 
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dyf = dfc.select(list(dfc.keys())[0])
    df = dyf.toDF()

 
    df = df.withColumnRenamed("Product Name", "Product_Name") \
           .withColumnRenamed("Maximum Spin Speed", "Maximum_Spin_Speed") \
           .withColumnRenamed("Washing Capacity", "Washing_Capacity") \
           .withColumnRenamed("Brand Name", "Brand_Name") \
           .withColumnRenamed("Model Name", "Model_Name") \
           .withColumnRenamed("Function Type", "Function_Type") \
           .withColumnRenamed("Inbuilt Heater", "Inbuilt_Heater") \
           .withColumnRenamed("Washing Method", "Washing_Method") \
           .withColumnRenamed("Product_Url", "Product_Url")

 
    df = df.withColumn("Brand_Name_Extracted", F.regexp_extract(F.col("Product_Name"), r'^(\w+)', 1))
    df = df.withColumn("Price_Rs", F.regexp_replace(F.col("Price"), "[₹,]", "").cast("double"))
    df = df.withColumn("Washing_Capacity_kg", F.regexp_replace(F.col("Washing_Capacity"), " kg", "").cast("double"))
    df = df.withColumn("Max_Spin_RPM", F.regexp_replace(F.col("Maximum_Spin_Speed"), " rpm", "").cast("double"))

     
    df = df.select(
        "Product_Name",
        "Brand_Name_Extracted",
        "Price_Rs",
        "Washing_Capacity_kg",
        "Max_Spin_RPM",
        "Color",
        "Function_Type",
        "Inbuilt_Heater",
        "Washing_Method",
 
    )

    dyf_out = DynamicFrame.fromDF(df, glueContext, "dyf_out")
    
    
    return DynamicFrameCollection({"CustomTransform0": dyf_out}, glueContext)

 
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://washing-machine-raw-data/data/Original_Washingmachine_dataset.csv"], "recurse": True},
    transformation_ctx="AmazonS3_node"
)
 
Data_Cleaning_Transform_node = MyTransform(glueContext, DynamicFrameCollection({"AmazonS3_node": AmazonS3_node}, glueContext))
 
dyf_cleaned = Data_Cleaning_Transform_node.select("CustomTransform0")
 
glueContext.write_dynamic_frame.from_options(
    frame=dyf_cleaned,
    connection_type="s3",
    connection_options={"path": "s3://washing-machine-raw-data/data/cleaned/"},
    format="csv"  # or "parquet"
)

job.commit()
