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

object ProgramPivot1 {
  
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
    
    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
                   ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
                   ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
                   ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

    val df = data.toDF("Product","Amount","Country")
    df.show() 
    
    //Pivot Spark DataFrame - pivot function rotates the data from one column into multiple columns.
    val pivotDF1 = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF1.show()
    
    //Pivot Performance improvement in Spark 2.0
    val countries = Seq("USA","China","Canada","Mexico")
    val pivotDF2 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF2.show()
    
    //Pivot using two-phase aggregation
    val pivotDF3 = df.groupBy("Product","Country")
                    .sum("Amount")
                    .groupBy("Product")
                    .pivot("Country")
                    .sum("sum(Amount)")
    pivotDF3.show()

    //Unpivot Spark DataFrame
    val unPivotDF = pivotDF1.select($"Product", expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
                           .where("Total is not null")
    unPivotDF.show()  
    
  
   }
  
}