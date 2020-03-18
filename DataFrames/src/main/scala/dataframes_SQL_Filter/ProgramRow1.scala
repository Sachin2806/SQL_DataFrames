package dataframes_SQL_Filter

import org.apache.spark.sql.Row

object ProgramRow1 {
   
  def main(args: Array[String]){
  
    val row = Row(1, "hello")
    
    println(row(0))
    println(row(1))
    
    val row1 =  Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M")
    
    println(row1(0))
    println(row1(1))
    println(row1(2))
    println(row1(3))
    
     val row2 = Row(1, true, "a string", null)
     println(row2(0))
     println(row2(1))
     println(row2(2))
     println(row2(3))
 
    
   }
}