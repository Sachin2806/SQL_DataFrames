package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ProgramJson5 {
  
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
  
  val df = spark.read.json(sc.wholeTextFiles("C:/Users/CSC/workspace/DataFrames/IO/people.json").values)  
  df.show()
  
  val flattened  = df.select($"name", explode($"schools").as("schools_flat"))
  flattened .show()
  
  val schools = flattened.select("name", "schools_flat.sname")
  schools.show()
  }
}