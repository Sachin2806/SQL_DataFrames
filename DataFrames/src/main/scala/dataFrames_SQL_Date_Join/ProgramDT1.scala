package dataFrames_SQL_Date_Join

import org.apache.spark.sql.types.DataType
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DateType

object ProgramDT1 {
  
  def main(args: Array[String]){
    
    //Spark SQL DataType - ArrayType
    val arr = ArrayType(IntegerType, false)
    println("<----ArrayType methods : ---->")
    println("json() 					: " + arr.json)  // Represents json string of datatype
    println("prettyJson() 		: " + arr.prettyJson) // Gets json in pretty format
    println("simpleString() 	: " + arr.simpleString) // simple string
    println("sql() 						: " + arr.sql) // SQL format
    println("typeName()				: " + arr.typeName) // type name
    println("catalogString()	: " + arr.catalogString) // catalog string
    println("defaultSize() 		: " + arr.defaultSize) // default size
    
    //DataType.fromJson()
    println()
    println("Data Type convrsion from Json string to Struct Type")
    val typeFromJson = DataType.fromJson(
    """{"type":"array","elementType":"string","containsNull":false}""".stripMargin)
    println(typeFromJson.getClass)
    
    val typeFromJson2 = DataType.fromJson("\"string\"")
    println(typeFromJson2.getClass)
    
    println()   
    println("<----StringType methods : ---->")
    val strType = DataTypes.StringType
    println("json : "+strType.json)
    println("prettyJson : "+strType.prettyJson)
    println("simpleString : "+strType.simpleString)
    println("sql : "+strType.sql)
    println("typeName : "+strType.typeName)
    println("catalogString : "+strType.catalogString)
    println("defaultSize : "+strType.defaultSize)
    
    println()   
    println("<----ArrayType methods : ---->")
    val arr1 = ArrayType(IntegerType,false)
    val arrayType = DataTypes.createArrayType(StringType,true)
    println("containsNull : "+arrayType.containsNull)
    println("elementType : "+arrayType.elementType)
    println("productElement : "+arrayType.productElement(0))

    println()   
    println("<----Map Type methods : ---->")
    val mapType1 = MapType(StringType,IntegerType)
    val mapType = DataTypes.createMapType(StringType,IntegerType)
    println("keyType() : "+mapType.keyType)
    println("valueType() : "+mapType.valueType)
    println("valueContainsNull() : "+mapType.valueContainsNull)
    println("productElement(1) : "+mapType.productElement(1))
    
    println()   
    println("<----StructType methods : ---->")
    
  //StructType
  val structType = DataTypes.createStructType(
    Array(DataTypes.createStructField("fieldName",StringType,true)))

  val simpleSchema = StructType(Array(
                     StructField("name",StringType,true),
                     StructField("id", IntegerType, true),
                     StructField("gender", StringType, true),
                     StructField("salary", DoubleType, true)))

  val anotherSchema = new StructType()
                      .add("name",new StructType()
                      .add("firstname",StringType)
                      .add("lastname",StringType))
                      .add("id",IntegerType)
                      .add("salary",DoubleType)
  
  
                      
    
    
  }
}