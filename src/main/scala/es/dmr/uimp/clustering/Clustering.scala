package es.dmr.uimp.clustering

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 3/12/17.
  */
object Clustering {
  /**
    * Load data from file, parse the data and normalize the data.
    */
  def loadData(sc: SparkContext, file : String) : DataFrame = {
    val sqlContext = new SQLContext(sc)

    // Function to extract the hour from the date string
    val gethour =  udf[Double, String]((date : String) => {
      var out = -1.0
      if (!StringUtils.isEmpty(date)) {
        val hour = date.substring(10).split(":")(0)
        if (!StringUtils.isEmpty(hour))
          out = hour.trim.toDouble
      }
      out
    })

    // Load the csv data
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(file)
      .withColumn("Hour", gethour(col("InvoiceDate")))

    df
  }

  def featurizeData(df : DataFrame) : DataFrame = {
    // Parse InvoiceDate with "MM/dd/yyyy HH:mm" format
    val dfWithTimestamp = df.withColumn(
      "InvoiceTimestamp",
      to_timestamp(col("InvoiceDate"), "MM/dd/yyyy HH:mm")
    )

    // Parse timestamp to hour with decimal minutes
    val dfWithHour = dfWithTimestamp.withColumn(
      "Time",
      hour(col("InvoiceTimestamp")) + minute(col("InvoiceTimestamp")) / 60.0
    )

    // Flag column to check if CustomerID is valid
    val dfWithCustomerFlag = dfWithHour.withColumn(
      "HasCustomer",
      when(col("CustomerID").isNull || trim(col("CustomerID")) === "", 0.0).otherwise(1.0)
    )

    // Aggregate rows by InvoiceNo
    val aggregatedDF = dfWithCustomerFlag.groupBy("InvoiceNo")
      .agg(
        avg("UnitPrice").alias("AvgUnitPrice"),
        min("UnitPrice").alias("MinUnitPrice"),
        max("UnitPrice").alias("MaxUnitPrice"),
        avg("Time").alias("Time"),
        sum("Quantity").alias("NumberItems"),
        max("HasCustomer").alias("HasCustomer")
      )
      // Cast columns to double
      .withColumn("AvgUnitPrice", col("AvgUnitPrice").cast(DoubleType))
      .withColumn("MinUnitPrice", col("MinUnitPrice").cast(DoubleType))
      .withColumn("MaxUnitPrice", col("MaxUnitPrice").cast(DoubleType))
      .withColumn("Time", col("Time").cast(DoubleType))
      .withColumn("NumberItems", col("NumberItems").cast(DoubleType))
      .withColumn("HasCustomer", col("HasCustomer").cast(DoubleType))

    aggregatedDF
  }

  def filterData(df: DataFrame): DataFrame = {
    df.filter(
        // Cancelled invoices
        !col("InvoiceNo").startsWith("C") &&
        // Invalid value
        col("AvgUnitPrice").isNotNull &&
        col("MinUnitPrice").isNotNull &&
        col("MaxUnitPrice").isNotNull &&
        col("NumberItems").isNotNull &&
        col("Time").isNotNull &&
        // Has a valid customer
        col("HasCustomer") === 1.0 &&
        // Out of valid range
        col("AvgUnitPrice") > 0 &&
        col("MinUnitPrice") > 0 &&
        col("MaxUnitPrice") > 0 &&
        col("NumberItems") > 0 &&
        col("Time").between(0, 24)
    )
    .drop("HasCustomer")
  }

  def toDataset(df: DataFrame): RDD[Vector] = {
    val data = df.select("AvgUnitPrice", "MinUnitPrice", "MaxUnitPrice", "Time", "NumberItems").rdd
      .map(row =>{
        val buffer = ArrayBuffer[Double]()
        buffer.append(row.getAs("AvgUnitPrice"))
        buffer.append(row.getAs("MinUnitPrice"))
        buffer.append(row.getAs("MaxUnitPrice"))
        buffer.append(row.getAs("Time"))
        buffer.append(row.getAs("NumberItems"))
        val vector = Vectors.dense(buffer.toArray)
        vector
      })

    data
  }

  def elbowSelection(costs: Seq[Double], ratio: Double): Int = {
    // Look up for first k where error(k) / error(k-1) > ratio
    for (i <- 1 until costs.length) {
      val currentRatio = costs(i) / costs(i - 1)
      if (currentRatio > ratio) {
        return i + 1 // +1 because i is 0-index and k 1-index
      }
    }

    // If no calculated ratio is higher than given ratio, return highest k
    costs.length
  }

  def saveThreshold(threshold : Double, fileName : String) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    // decide threshold for anomalies
    bw.write(threshold.toString) // last item is the threshold
    bw.close()
  }

}
