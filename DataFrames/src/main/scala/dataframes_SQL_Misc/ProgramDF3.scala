package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ProgramDF3 {
  
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
    
    //DataFrame First Row
    val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
    val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price")
    df.show()

    val firstRow = df.first()
    println(s"First row = $firstRow")

    val firstRowColumn1 = df.first().get(0)
    println(s"First row column 1 = $firstRowColumn1")

    val firstRowColumnPrice = df.first().getAs[Double]("Price")
    println(s"First row column Price = $firstRowColumnPrice")
    
    //Format DataFrame column
    val donuts1 = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
    val df1 = spark.createDataFrame(donuts1).toDF("Donut Name", "Price", "Purchase Date")
  
    df1
    .withColumn("Price Formatted", format_number($"Price", 2))
    .withColumn("Name Formatted", format_string("awesome %s", $"Donut Name"))
    .withColumn("Name Uppercase", upper($"Donut Name"))
    .withColumn("Name Lowercase", lower($"Donut Name"))
    .withColumn("Date Formatted", date_format($"Purchase Date", "yyyyMMdd"))
    .withColumn("Day", dayofmonth($"Purchase Date"))
    .withColumn("Month", month($"Purchase Date"))
    .withColumn("Year", year($"Purchase Date"))
    .show()
    
    //DataFrame column hashing
//    val donuts2 = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
//    val df2 = spark.createDataFrame(donuts2).toDF("Donut Name", "Price", "Purchase Date")
//    
//    df2
//      .withColumn("Hash", hash($"Donut Name")) // murmur3 hash as default.
//      .withColumn("MD5", md5($"Donut Name"))
//      .withColumn("SHA1", sha1($"Donut Name"))
//      .withColumn("SHA2", sha2($"Donut Name", 256)) // 256 is the number of bits
//      .show()
    
    //DataFrame String Functions  
    val donuts3 = Seq(("plain donut", 1.50, "2018-04-17"), ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))
    val df3 = spark.createDataFrame(donuts3).toDF("Donut Name", "Price", "Purchase Date")
	   
    df3
      .withColumn("Contains plain", instr($"Donut Name", "donut"))
      .withColumn("Length", length($"Donut Name"))
      .withColumn("Trim", trim($"Donut Name"))
      .withColumn("LTrim", ltrim($"Donut Name"))
      .withColumn("RTrim", rtrim($"Donut Name"))
      .withColumn("Reverse", reverse($"Donut Name"))
      .withColumn("Substring", substring($"Donut Name", 0, 5))
      .withColumn("IsNull", isnull($"Donut Name"))
      .withColumn("Concat", concat_ws(" - ", $"Donut Name", $"Price"))
      .withColumn("InitCap", initcap($"Donut Name"))
      .show()
  
   //DataFrame drop null
   val donuts4 = Seq(("plain donut", 1.50), (null.asInstanceOf[String], 2.0), ("glazed donut", 2.50))
   val dfWithNull = spark.createDataFrame(donuts4).toDF("Donut Name", "Price")

   dfWithNull.show()
   
      
  }
}