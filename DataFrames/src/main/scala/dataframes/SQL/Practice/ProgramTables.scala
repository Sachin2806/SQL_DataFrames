package dataframes.SQL.Practice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ProgramTables {
  
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
    
    //val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
    //val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price")
    //df.createGlobalTempView("global_temp.test")
    //spark.sql("SELECT * FROM global_temp.test").show()
    //spark.newSession().sql('select * from global_temp.test').show()
    
//    val df = spark.read.json("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/employee.json")   
//    df.createOrReplaceTempView("employee1")
//    val sqlDF = spark.sql("SELECT * FROM employee1 ")
//    println("createOrReplaceTempView")
//    sqlDF.show()
//    
//    //df.createGlobalTempView("people2")
//    val sqlDF2 = spark.sql("SELECT * FROM global_temp.people2")
//    sqlDF2.show()
    
  }
}