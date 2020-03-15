package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ProgramJson4 {
  
  def main(args: Array[String])
  {
    
   val spark = SparkSession
              .builder()
              .appName("ProgramJson3")
              .config("spark.master", "local")
              .config("spark.sql.warehouse.dir", "file:///C:/Users/CSC/git/SparkSQL/Scala_DataFrames/spark-warehouse")
              .getOrCreate()
  
  val sc = spark.sparkContext
  import spark.implicits._
  
  val jsonData_1 = spark.read.json("C:/Users/CSC/workspace/DataFrames/IO/employees_singleLine.json")
  val jsonData_2 = spark.read.json(sc.wholeTextFiles("C:/Users/CSC/workspace/DataFrames/IO/employees_multiLine.json").values)
  
  jsonData_1.show()
  jsonData_2.show()
  
  //except function has used to compare both the data frames.
  jsonData_1.except(jsonData_2).show
  
  //Selecting only "ename" columns
  jsonData_1.select("ename").show()
  
  jsonData_1.select("deptno").distinct.show()
  jsonData_1.registerTempTable("employeeTb1")
  spark.sql("select distinct deptno from employeeTb1").show()
  
  
  
  
  
  
  }
}