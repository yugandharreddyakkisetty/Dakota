import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object Interview {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Interview").getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._
    var path="D://bucket/LEVEL1/LEVEL2/LEVEL3/LEVEL4/sample.csv"

//    path="D://ctms-corning-spark/CTMS/SHIPMENT/ELEMENTUM/DAILY/Corning_LV_Report_2020-06-10.csv"
//    path="D://bucket/LEVEL1/LEVEL2/LEVEL3/LEVEL4/data-1593071637840.csv"




    val df = spark.read
      .option("header", true)
      .option("inferSchema",true)
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(path)

    df.show()

  /*

    // groupby count and first
    df.groupBy("lower").agg(countDistinct("ocean_l1q_ou")).show()
df.filter(col("lower") === "ups" && col("lower-3") ==="lima").show()
val rightDf=df.withColumn("rate_effective_date1",unix_timestamp(col("rate_effective_date"),"yyyy-MM-dd hh:mm:ss"))
  .withColumn("rate_expiry_date1",unix_timestamp(col("rate_expiry_date"),"yyyy-MM-dd hh:mm:ss"))

    val DF=Seq(("ups","winston salem","lima","2020-02-06 00:00:00"),("ups","winston salem","lima","2020-01-01 00:00:00"))
      .toDF("carrier","origin","destination","date")
    DF.show()
    val leftDF=DF.withColumn("date1",unix_timestamp(col("date")))
val expression="`lower` == `carrier` and `origin` == `lower-2` and `destination` == `lower-3` and `date1` >= `rate_effective_date1` and `date1` <= `rate_expiry_date1`"

    leftDF.join(rightDf,expr(expression),"left")
      .drop("lower","lower-2","lower-3","rate_effective_date","rate_effective_date1","count","rate_expiry_date1","rate_expiry_date","date1")
      .groupBy("carrier","origin","destination","date").agg(countDistinct("ocean_l1q_ou"),first("ocean_l1q_ou") ).show
*/
// replace and regular expression
/*    df
      .withColumn("Quantity-1",regexp_replace(col("AccountingDate"),"[^0-9-:\t ]+",""))
      .show()*/

    df
      .withColumn("City-1",regexp_replace(col("City"),"[^0-9a-zA-Z-!@#$%^&*()_=+\\[\\]?\"\':;<>.]+",""))
      .show(20,false)
    val exp="length(col(\"City\"))>4000"
    df.withColumn("length",when(col("City").rlike("[Uu][Nn][Kk][Nn][Oo][Ww][Nn]"),lit("more")).otherwise(lit("less"))).show(10)
    df.withColumn("Sqeeze",regexp_replace(col("City"),"\\s+"," ")).show(10,false)
    df.withColumn("DateRegexp",when(col("AccountingDate").rlike("^\\d\\d\\d\\d-\\d\\d-\\d\\d|NULL"),lit("more")).otherwise(lit("less"))).show(10)
    df.withColumn("current",current_timestamp()).show(10,false)

    df
      .withColumn("NULL_COLUMN",lit(null).cast("string"))
      .withColumn("pattern",lit("^(?!\\d\\d\\d\\d-\\d\\d-\\d\\d).*"))
      .withColumn("AccountingDate2",regexp_replace(col("AccountingDate"),"^(?!\\d\\d\\d\\d-\\d\\d-\\d\\d).*",""))
      .withColumn("AccountingDate3",regexp_replace(col("AccountingDate"), col("pattern"),col("NULL_COLUMN")))
      .withColumn("InvoiceLineNumber",col("InvoiceLineNumber").cast(IntegerType))
      .printSchema()
      //.show(20,false)
    val expression="mode != \"AIR\" and mode != \"SEA\" and mode != \"FTL\" and mode != \"LTL\""
    df.filter(expr(expression)).show()


 /*
    // Find minimum and maximum of a column in data frame
    print(df.schema)
        val ll=List("min","max")
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

/*
 // concat multiple columns
    val logic="Quantity|InvoiceId|InvoiceLineNumber|City"
    val list=logic.split("\\|").map(x=>col(x)).toList

    df.withColumn("ConcatExpression",concat(list:_*)).show(10,false)

// select all the column names ending with _flag
    df.select(df.columns.filter(x=>x.endsWith("Id")).map(df(_)) : _*).show


*/

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
