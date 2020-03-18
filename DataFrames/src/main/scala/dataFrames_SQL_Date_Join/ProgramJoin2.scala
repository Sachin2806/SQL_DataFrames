package dataFrames_SQL_Date_Join

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.functions.col

object ProgramJoin2 {
  
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
    
    val emp = Seq((1,"Smith",-1,"2018","10","M",3000), (2,"Rose",1,"2010","20","M",4000),
                  (3,"Williams",1,"2010","10","M",1000),(4,"Jones",2,"2005","10","F",2000),
                  (5,"Brown",2,"2010","40","",-1),(6,"Brown",2,"2010","50","",-1))
  
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined", "emp_dept_id","gender","salary")
  
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept = Seq(("Finance",10),("Marketing",20),("Sales",30),("IT",40))
    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    deptDF.show(false)
    
    //Spark SQL Join
    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")
    
    //SQL JOIN
    val joinDF1 = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    joinDF1.show()
    
    val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
    joinDF2.show()
    
    println("Inner join")
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),Inner.sql).orderBy("emp_id").show(false)
  }
}