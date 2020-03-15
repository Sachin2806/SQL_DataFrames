package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ProgramJson3 {
  
  def main(args: Array[String])
  {
    
   val spark = SparkSession
              .builder()
              .appName("ProgramJson3")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  val tagsDF = spark
              .read
              .option("multiLine", true)
              .option("inferSchema", true)
              .json("C:/Users/CSC/workspace/DataFrames/IO/sample.json")
  
  tagsDF.show()
  
  }
}