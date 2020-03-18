package dataFrames_SQL_Write

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object ProgramWrite4 {
  
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
    
    val multiline_df = spark.read.json(sc.wholeTextFiles("C:/Users/CSC/git/SQL_DataFrames/DataFrames/IO/Input/multiline_zipcode.json").values)
    multiline_df .show()
    
    multiline_df.write.json("C:/Users/CSC/git/SQL_DataFrames/DataFrames/IO/Output/multiline_zipcode_Out.json")
  }
}