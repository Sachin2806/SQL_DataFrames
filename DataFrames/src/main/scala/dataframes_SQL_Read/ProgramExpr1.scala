package dataframes_SQL_Read

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.expr

object ProgramExpr1 {
  
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

    val df = Seq((1,2)).toDF("x","y")
    val myExpr = "x + y"
    
    df.withColumn("z", expr(myExpr)).show()
    
  }
}