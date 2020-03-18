package dataframes_SQL_Misc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ProgramDF4 {
  
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
    
    // Create the Departments
    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")

    //Create the Employees
    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)
    val employee5 = new Employee("michael", "jackson", "no-reply@neverla.nd", 80000)

    // Create the DepartmentWithEmployees instances from Departments and Employees
    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
    val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee5, employee4))
    val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))
    
    val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
    val df1 = departmentsWithEmployeesSeq1.toDF()
    df1.show()
    
    val explodeDF = df1.select(explode($"employees")).as("Employees")
    val flattenDF = explodeDF.select($"col.*")
    flattenDF.show()
    
    //val a1 = df1.filter(df1("department.id") === 123456).show()   
      
//    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
//    val df2 = departmentsWithEmployeesSeq2.toDF()
//    df2.show()
//    
//    val unionDF = df1.union(df2)
//    unionDF.show()
   }
  
  // Create the case classes for our domain
    case class Department(id: String, name: String)
    case class Employee(firstName: String, lastName: String, email: String, salary: Int)
    case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])
}