package dataframes_SQL_Filter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ProgramAggr1 {
    
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
    
    val simpleData = Seq(("James", "Sales", 3000), ("Michael", "Sales", 4600),
                         ("Robert", "Sales", 4100),("Maria", "Finance", 3000),
                         ("James", "Sales", 3000), ("Scott", "Finance", 3300),
                         ("Jen", "Finance", 3900), ("Jeff", "Marketing", 3000),
                         ("Kumar", "Marketing", 2000), ("Saif", "Sales", 4100))
   
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.show()                        
    
    //approxCountDistinct()
    println("approx_count_distinct: " +  df.select(approxCountDistinct("salary")).collect()(0)(0))
    
    //Average
    println("avg : " + df.select(avg("salary")).collect()(0)(0))
    
    //collect_list
    df.select(collect_list("salary")).show(false)

    //collect_set
    df.select(collect_set("salary")).show(false)

    //countDistinct
    val df2 = df.select(countDistinct("department", "salary"))
    df2.show(false)
    println("Distinct Count of Department & Salary: "+df2.collect()(0)(0))

    println("count: " + df.select(count("salary")).collect()(0))

    //first
    df.select(first("salary")).show(false)

    //last
    df.select(last("salary")).show(false)
    df.select(kurtosis("salary")).show(false)
    df.select(max("salary")).show(false)
    df.select(min("salary")).show(false)
    df.select(mean("salary")).show(false)
    df.select(skewness("salary")).show(false)
    df.select(stddev("salary"), stddev_samp("salary"),stddev_pop("salary")).show(false)
    df.select(sum("salary")).show(false)
    df.select(sumDistinct("salary")).show(false)
    df.select(variance("salary"),var_samp("salary"),var_pop("salary")).show(false)
    //Exception in thread "main" org.apache.spark.sql.AnalysisException: 
    // grouping() can only be used with GroupingSets/Cube/Rollup;
    //df.select(grouping("salary")).show(false)


}


}