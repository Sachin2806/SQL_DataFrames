package dataFrames_SQL_Write

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object ProgramWrite2 {
  
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
    
    val data = Seq(("James ","","Smith","36636","M",3000),
                   ("Michael ","Rose","","40288","M",4000),
                   ("Robert ","","Williams","42114","M",4000),
                   ("Maria ","Anne","Jones","39192","F",4000),
                   ("Jen","Mary","Brown","","F",-1))
    
    val columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    val df = data.toDF(columns:_*)
    
    df
    .write
    .partitionBy("gender","salary")
    .parquet("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/people.parquet")
     
    //Details of options in write like "mode" and "partitionBy"
    //.mode("append") - Program doesnt fail and it appends the data at the end.
    //.partitionBy("gender","salary") - Creates 2 partitions with "gender" and "salary" within the actual file
    
    val parqDF = spark.read.parquet("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/people.parquet")
    parqDF.printSchema()
    
    parqDF.createOrReplaceTempView("ParquetTable")
    val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")
    parkSQL.show()
    
    val parqDF1 = spark.read.parquet("C:/Users/CSC/git/SparkSQL/Scala_DataFrames/Files/people.parquet/gender=M")
    parqDF1.show()
  }
}