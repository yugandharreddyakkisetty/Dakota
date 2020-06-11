import org.apache.spark.sql.{SaveMode, SparkSession,Row}
import org.apache.spark.sql.functions._


object Interview {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Interview").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val df = spark.read
      .option("header", true)
      .option("inferSchema",true)
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv("D://bucket/LEVEL1/LEVEL2/LEVEL3/LEVEL4/sample1.csv")
    val ll=List("min","max")

 /*
    // Find minimum and maximum of a column in data frame
 println(df.count)
    df.schema.toList.foreach{
      i=>{
        println(i)
      }
    }

    println(df.describe().show(10))

    val l=df.describe().filter(col("summary").isin(ll:_*)).collect().toList.map(r=>r.getString(0)->r).toMap
    println(l)
    l.foreach(println(_))
    val minRow:Row=l("min")
    val maxRow:Row=l("max")

    val index=minRow.fieldIndex("InvoiceId")
    println(minRow.getString(index))

*/

 // concat multiple columns
    val logic="Quantity|-|InvoiceId|InvoiceLineNumber|City"
    val list=logic.split("\\|")
    val zippedList=list.zipWithIndex
    val concatExpression=zippedList.map{
      case (item,index) => {
        if(index/2 == 0) col(item) else lit(item)
      }
    }

    concatExpression.foreach(println(_))
    println(concatExpression)
    df.withColumn("ConcatExpression",concat(col("Quantity"),lit("-"))).show

// select all the column names ending with _flag
    df.select(df.columns.filter(x=>x.endsWith("Id")).map(df(_)) : _*).show



/*

    val pullnumeric = udf {
      (input: String) => {
        input.split("-")(1)
      }
    }

    val numbered_date = udf {
      (input: String) =>
        val date_array = input.split(" ")(0).split("-")
        date_array.map(_.toInt).sum
    }
    // column rename
    df.withColumnRenamed("AccountingDate", "Date").show()
    // copy one column to other column
    df.withColumn("NewColumn", col("AccountingDate")).show()
    // changing date format
    df.withColumn("AccountingDate", date_format(to_date(col("AccountingDate")), "MM/dd/yyyy")).show
    // pull numeric part
    df.withColumn("ERPCommodityIdNumeric_part", pullnumeric(col("ERPCommodityId"))).show
    // date sum == dd+mm+yyyy
    df.withColumn("NumberedDate", numbered_date(col("AccountingDate"))).show()

    // group by
    df.select("*").groupBy("InvoiceLineNumber").agg(sum(col("Quantity")).as("Sum")).show()

    // select all columns
    df.select("*").show

    // replace a character in all the columns
    val columns = df.columns
    val df2 = columns.foldLeft(df) { (tempdf, column_name) => {
            tempdf.withColumn(column_name, regexp_replace(col(column_name),"-", "\"")).withColumn(column_name,regexp_replace(col(column_name),"\"","\"\"\""))
      }
    }
    df2.show()
*/
//    df2.write.option("header","true").option("charset","UTF-8").option("escape", "\"").mode(SaveMode.Overwrite).csv("csv_output/temp_test")

  }

}
