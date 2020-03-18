package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ProgramDF2 {
  
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
    
    //Rename DataFrame column
    val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
    val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price")
    df.show()

    val df1 = df.withColumnRenamed("Donut Name", "Name")
    df1.show()
  
    //Create DataFrame constant column
//    val df2 = df
//              .withColumn("Tasty", lit(true))
//              .withColumn("Correlation", lit(1))
//              .withColumn("Stock Min Max", typedLit(Seq(100, 500)))
//
//    df2.show()
    
    val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match 
    {  
      case "plain donut"    => Seq(100, 500)
      case "vanilla donut"  => Seq(200, 400)
      case "glazed donut"   => Seq(300, 600)
      case _                => Seq(150, 150)
    }

    val udfStockMinMax = udf(stockMinMax)
    val df2 = df.withColumn("Stock Min Max", udfStockMinMax($"Donut Name"))
    df2.show()
  
  }
}