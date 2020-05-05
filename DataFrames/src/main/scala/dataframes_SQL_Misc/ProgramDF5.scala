package dataframes_SQL_Misc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row

object ProgramDF5 {
  
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
    
    val simpleData = Seq(("James", "Sales", 3000), ("Michael", "Sales", 4600),
                         ("Robert", "Sales", 4100),("Maria", "Finance", 3000),
                         ("James", "Sales", 3000), ("Scott", "Finance", 3300),
                         ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000),
                         ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100))
                         
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()
  
    //row_number Window Function
    val windowSpec  = Window.partitionBy("department").orderBy("salary")
    df.withColumn("row_number",row_number.over(windowSpec)).show()
    
    //rank Window Function
    df.withColumn("rank", rank().over(windowSpec)).show()
    
    //dense_rank Window Function
    df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
    
    //percent_rank Window Function
    df.withColumn("percent_rank", percent_rank().over(windowSpec)).show()
    
    //ntile
    df.withColumn("ntile", ntile(2).over(windowSpec)).show()
    
    //cume_dist Window Function
    df.withColumn("cume_dist",cume_dist().over(windowSpec)).show()
    
    //lag
    df.withColumn("lag",lag("salary",1).over(windowSpec)).show()
    
    //lead
    df.withColumn("lead",lead("salary",1).over(windowSpec)).show()
    
    //Spark Window Aggregate Functions
    val windowSpecAgg  = Window.partitionBy("department")
    val aggDF = df.withColumn("row",row_number.over(windowSpec))
                  .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
                  .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
                  .withColumn("min", min(col("salary")).over(windowSpecAgg))
                  .withColumn("max", max(col("salary")).over(windowSpecAgg))
                  .where(col("row") === 1).select("department","avg","sum","min","max")
                  .show()
    
   }
  
}