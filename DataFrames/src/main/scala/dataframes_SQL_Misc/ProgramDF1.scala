package dataframes_SQL

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


object ProgramDF1 {
  
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
    
    //Create a dataframe which represents a donut id, name and price.
    val donuts = Seq(("111","plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333","glazed donut", 2.50))
    val dfDonuts = spark.createDataFrame(donuts).toDF("Id","Donut Name", "Price")
    dfDonuts.show()
    
    //Create another dataframe which represents a donut id and an inventory amount.
    val inventory = Seq(("111", 10), ("222", 20), ("333", 30))
    val dfInventory = spark.createDataFrame(inventory).toDF("Id", "Inventory")
    dfInventory.show()
  
    //Concatenate DataFrames using join()
    val dfDonutsInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
    dfDonutsInventory.show()
    
    //Check DataFrame column exists
    val donuts1 = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))
    val df1 = spark.createDataFrame(donuts1).toDF("Donut Name", "Price")
    
    val priceColumnExists = df1.columns.contains("Price")
    println(s"Does price column exist = $priceColumnExists")
    
    //Split DataFrame Array column
    val targets = Seq(("Plain Donut", Array(1.50, 2.0)), ("Vanilla Donut", Array(2.0, 2.50)), ("Strawberry Donut", Array(2.50, 3.50)))
    val df2 = spark.createDataFrame(targets).toDF("Name", "Prices")

    df2.show()
    
    val df3 = df2.select($"Name",$"Prices"(0).as("Low Price"),$"Prices"(1).as("High Price"))
    df3.show()
  }
}