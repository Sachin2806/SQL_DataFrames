package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ProgramJsonTest {
  
  def main(args: Array[String])
  {
    
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

    val readJSON = sc.wholeTextFiles("C:/Users/CSC/workspace/DataFrames/IO/sample.json")
    .map(x => x._2)
    .map(data => data.replaceAll("\n", ""))
         
    val df = spark.read.json(readJSON)
    df.show()
    
    val flattened  = df.select(explode($"content").as("content_flat"))
    flattened .show()
  
    val dfDates = df.select(explode(df("dates"))).toDF("dates")    
    dfDates.show()
    
    val dfContent = df.select(explode(df("content"))).toDF("content")
    dfContent.show()
  }
}