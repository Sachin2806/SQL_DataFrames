package dataFrames_SQL_Write

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object ProgramWrite1 {
  
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
    
    val baby_names = spark
                     .read
                     .format("com.databricks.spark.csv")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/Baby_Names__Beginning_2007.csv")
  
    //Basic write in spark in .csv format
    baby_names.write
               .format("csv")
               .mode("overwrite")
               .option("mode", "OVERWRITE")
               .option("dateFormat", "yyyy-MM-dd")
               .save("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/OutPut")
                                           
  }
}