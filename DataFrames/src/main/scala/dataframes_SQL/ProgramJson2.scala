package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ProgramJson2 {
  
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
    
    val readJSON = sc.wholeTextFiles("C:/Users/CSC/workspace/DataFrames/IO/tags_sample.json")
    .map(x => x._2)
    .map(data => data.replaceAll("\n", ""))
  
    val df = spark.read.json(readJSON)
    df.show()
    
    val df1 = df.select(explode($"stackoverflow") as "stackoverflow_tags")
   
    val df2 = df1.select(
    $"stackoverflow_tags.tag.id" as "id",
    $"stackoverflow_tags.tag.author" as "author",
    $"stackoverflow_tags.tag.name" as "tag_name",
    $"stackoverflow_tags.tag.frameworks.id" as "frameworks_id",
    $"stackoverflow_tags.tag.frameworks.name" as "frameworks_name"
  )
  df2.show()
  
  //Search DataFrame column using array_contains()
  val df3 = df2.select("*").where(array_contains($"frameworks_name","Play Framework"))
  df3.show()
  }
}