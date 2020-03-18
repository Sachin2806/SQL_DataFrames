package dataframes_SQL_Read

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

object ProgramRead3 {
  
  def main(args: Array[String])  {
    
    val conf = new SparkConf()
                    .setAppName("ProgramJson2")
                    .setMaster("local")
    val spark = SparkSession
                 .builder()
                 .appName("ProgramJson2")
                 .config(conf)
                 .config("spark.master", "local")
                 .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
                 .getOrCreate()
               
    val sc = spark.sparkContext
    import spark.implicits._

    //Reading files with a user-specified custom schema
    //Define custom schema
    val schema = new StructType()
	      .add("RecordNumber", LongType, true)
	      .add("Zipcode", StringType, true)
        .add("ZipCodeType", StringType, true)
	      .add("City", StringType, true)
	      .add("State", StringType, true)
	      .add("LocationType", StringType, true)
	      .add("Lat", DoubleType, true)
	      .add("Long", DoubleType, true)
	      .add("Xaxis", DoubleType, true)
        .add("Yaxis", DoubleType, true)
        .add("Zaxis", DoubleType, true)
	      .add("WorldRegion", StringType, true)
	      .add("Country", StringType, true)
	      .add("LocationText", StringType, true)
        .add("Location", StringType, true)
        .add("Decommisioned", BooleanType, true)
	      .add("TaxReturnsFiled", LongType, true)
        .add("EstimatedPopulation", LongType, true)
        .add("TotalWages", LongType, true)
        .add("Notes", StringType, true)
      
   //val df_with_schema = spark.read.schema(schema).json()
   val df_with_schema = spark
                        .read
                        .schema(schema)
                        .json("C:/Users/CSC/git/SQL_DataFrames/DataFrames/IO/Input/zipcodes.json")
        
   df_with_schema.printSchema()
   df_with_schema.show(false)
    
  }
}