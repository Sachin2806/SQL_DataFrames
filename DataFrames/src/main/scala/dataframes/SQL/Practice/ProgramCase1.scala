package dataframes.SQL.Practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when

object ProgramCase1 {
  
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
    
    //Using “when otherwise” on Spark DataFrame.
    
    val data = List(("James","","Smith","36636","M",60000),
        ("Michael","Rose","","40288","M",70000),
        ("Robert","","Williams","42114","",400000),
        ("Maria","Anne","Jones","39192","F",500000),
        ("Jen","Mary","Brown","","F",0))
    val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
    val df = spark.createDataFrame(data).toDF(cols:_*)
    
    val df1 = df.withColumn("new_gender", when(col("gender") === "M","Male")
                                         .when(col("gender") === "F","Female")
                                         .otherwise("Unknown"))
    df1.show()
    val df2 = df.select(col("*"), when(col("gender") === "M","Male")
                                 .when(col("gender") === "F","Female")
                                 .otherwise("Unknown")
                                 .alias("new_gender"))
    df2.show()
    
    //Using “case when” on Spark DataFrame.
    val df3 = df.withColumn("new_gender", 
      expr("case when gender = 'M' then 'Male' " + 
	                    "when gender = 'F' then 'Female'" +
	                    "else 'Unknown' end").alias("new_gender"))
	                                                   
	 val df4 = df.select(col("*"),
      expr("case when gender = 'M' then 'Male' " +
                       "when gender = 'F' then 'Female' " +
                       "else 'Unknown' end").alias("new_gender"))
                       
   //Using && and || operator
   val dataDF = Seq((66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"), (72, "e", "5")).toDF("id", "code", "amt")
   
   dataDF.withColumn("new_column",
       when(col("code") === "a" || col("code") === "d", "A")
      .when(col("code") === "b" && col("amt") === "4", "B")
      .otherwise("A1"))
      .show()
      
   println(df1.rdd.partitions.length)
    
  }    
}