package dataframes_SQL_Filter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ProgramFilter1 {
    
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
    

    val arrayStructureData = Seq(
                            Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
                            Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
                            Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
                            Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
                            Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
                            Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M"))

    val arrayStructureSchema = new StructType()
                                .add("name",new StructType()
                                .add("firstname",StringType)
                                .add("middlename",StringType)
                                .add("lastname",StringType))
                                .add("languages", ArrayType(StringType))
                                .add("state", StringType)
                                .add("gender", StringType)
                                
    val df = spark.createDataFrame(sc.parallelize(arrayStructureData),arrayStructureSchema)
    df.printSchema()
    df.show()
    
    //DataFrame filter() with Column condition
    df.filter(df("state") === "OH").show()
    
    //DataFrame filter() with SQL Expression
    df.filter("gender = 'M'").show()
    
    //Filtering with multiple condition
    df.filter(df("state") === "OH" && df("gender") === "M").show()
    
    //Filtering on an Array column
    df.filter(array_contains(df("languages"), "Java")).show()
    
    //Filtering on Nested Struct columns - Struct condition
    df.filter(df("name.firstname") === "James").show()
    
  }
  
}