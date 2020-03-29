package dataframes.SQL.Practice

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

object ProgramNst1 {
  
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
    
    //Ways to Rename column on Spark DataFrame
    
    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
                   Row(Row("Michael ","Rose",""),"40288","M",4000),
                   Row(Row("Robert ","","Williams"),"42114","M",4000),
                   Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
                   Row(Row("Jen","Mary","Brown"),"","F",-1))
                   
    val schema = new StructType()
      .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    
    val df = spark.createDataFrame(sc.parallelize(data),schema)
    df.printSchema()
    
    //Using Spark withColumnRenamed – To rename DataFrame column name
    df.withColumnRenamed("dob", "DateOfBirth").printSchema()
    
    //Using withColumnRenamed – To rename multiple columns
    val df1 = df.withColumnRenamed("salary", "Salary")
                .withColumnRenamed("dob", "DateOfBirth")
    df1.printSchema()
    
    //Using Spark StructType – To rename a nested column in Dataframe
     val schema1 = new StructType()
                  .add("fname",StringType)
                  .add("middlename",StringType)
                  .add("lname",StringType)
                  
     df.select(col("name").cast(schema1),col("dob"),col("gender"),col("salary")).printSchema()
     
     //Using Select – To rename nested elements.
     df.select(col("name.firstname").as("fname"),
               col("name.middlename").as("mname"),
               col("name.lastname").as("lname"),
               col("dob"),col("gender"),col("salary")).printSchema()
     
     //Using Spark DataFrame withColumn – To rename nested columns
     val df4 = df.withColumn("fname", col("name.firstname"))
                 .withColumn("mname",col("name.middlename"))
                 .withColumn("lname",col("name.lastname"))
                 .drop("name")
     df4.printSchema()
     
     //Using col() function – To Dynamically rename all or multiple columns
     val old_columns = Seq("dob","gender","salary","fname","mname","lname")
     val new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
     val columnsList = old_columns.zip(new_columns).map(f=>{col(f._1).as(f._2)})
     val df5 = df4.select(columnsList:_*)
     df5.printSchema()
     
     //Using toDF() – To change all columns in a Spark DataFrame
     val newColumns = Seq("newCol1","newCol2","newCol3")
     val df6 = df.toDF(newColumns:_*).printSchema()     
  }
}