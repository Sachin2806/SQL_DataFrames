package dataFrames_SQL_Date_Join

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.to_utc_timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProgramDate2 {
  
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
    
    //datediff :
    val df1 = Seq(("2020-03-14"),("2020-03-15"),("2020-03-16"))
             .toDF("input")
             .select(col("input"), current_date(),datediff(current_date(),col("input")).as("diff"))
             .show()
    
     //months_between :
     val df2 = Seq(("2020-01-14"),("2020-02-14"),("2020-03-01"))
               .toDF("date").select(col("date"), current_date(),
                datediff(current_date(),col("date")).as("datediff"),
                months_between(current_date(),col("date")).as("months_between"))
               .show() 
    
    //trunc: 
    val df3 = Seq(("2020-01-14"),("2020-02-14"),("2020-03-01"))
               .toDF("input").select(col("input"),
                trunc(col("input"),"Month").as("Month_Trunc"),
                trunc(col("input"),"Year").as("Month_Year"),
                trunc(col("input"),"Month").as("Month_Trunc"))
               .show()
               
    //add_months , date_add, date_sub
    val df4 =  Seq(("2020-01-14"),("2020-02-14"),("2020-03-01"))
              .toDF("input")
              .select(col("input"), add_months(col("input"),3).as("add_months"),
               add_months(col("input"),-3).as("sub_months"),
               date_add(col("input"),4).as("date_add"),
               date_sub(col("input"),4).as("date_sub"))
              .show()
              
    //year, month, month, dayofweek, dayofmonth, dayofyear, next_day, weekofyear

    val df5 = Seq(("2020-01-14"),("2020-02-14"),("2020-03-01"))
              .toDF("input").select(col("input"),
              year(col("input")).as("year"), month(col("input")).as("month"),
//            dayofweek(col("input")).as("dayofweek"),
              dayofmonth(col("input")).as("dayofmonth"),
              dayofyear(col("input")).as("dayofyear"),
              next_day(col("input"),"Sunday").as("next_day"),
              weekofyear(col("input")).as("weekofyear"))
             .show()
      
    val df = Seq(("2019-07-01 12:01:19.000"),("2019-06-24 12:01:19.000"),
                 ("2019-11-16 16:44:55.406"),("2019-11-16 16:50:59.406")).toDF("input_timestamp")
     
    //To get the current date/timestamp, day number of the week and the day
    df.withColumn("input_timestamp",current_timestamp())
      .withColumn("Day Num of the week", date_format(col("input_timestamp"), "u"))
      .withColumn("Day of the week", date_format(col("input_timestamp"), "E"))
      .show(false)
        
    
  }
}