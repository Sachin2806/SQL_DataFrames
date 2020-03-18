package dataFrames_SQL_Date_Join

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.current_date

object ProgramDate1 {
  
  def main(args: Array[String]){
    
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
    
    //current_date and date_format
    val df1 = Seq(("2020-03-16"))
               .toDF("Input")
               .select(current_date()as("current_date"), col("Input"),date_format(col("Input"), "MM-dd-yyyy")
               .as("format"))
               .show()
    
    //to_date :
    val df2 = Seq((1L, "17-MAR-2020")).toDF("date", "ts")
    df2.select(to_date(unix_timestamp($"ts", "dd-MMM-yyyy").cast("timestamp")).alias("Formatted_Date")).show()
    df2.dtypes    
  }
}