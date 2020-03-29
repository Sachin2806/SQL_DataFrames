package dataframes_SQL_Misc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import scala.collection.mutable.Stack

object ProgramPivot2 {
  
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
    
    val sourceData = Seq(("john", "notebook", 2), ("gary", "notebook", 3), ("john", "small phone", 2),
                       ("mary", "small phone", 3), ("john", "large phone", 3), ("john", "camera", 3))
    val df = sourceData.toDF("salesperson","device", "amount sold")
    df.show()
    
    val pivotDF = df.groupBy("salesperson").pivot("device").sum("amount sold")
    pivotDF.show()
    
    //Unpivot
    val source = Seq(("gary", None, None, Some(3),None), ("mary",None, None, None, Some(3)),
                        ("john",Some(3),Some(3),Some(2),Some(2)))
                        
    val sourceDF = source.toDF("salesperson", "camera", "large_phone", "notebook", "small_phone")
    sourceDF.show()
    
    val unpivotedDf = sourceDF
    .selectExpr("salesperson","stack(4,'camera',camera,'large phone',large_phone,'notebook',notebook,'small_phone',small_phone)")
    .withColumnRenamed("col0","device") // default name of this column is col0
    .withColumnRenamed("col1","amount_sold") // default name of this column is col1
    .filter($"amount_sold".isNotNull) // must explicitly remove nulls
                       
    unpivotedDf.show()     
   }
  
}