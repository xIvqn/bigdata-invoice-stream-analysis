package es.dmr.uimp.realtime

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object InvoicePipeline {

  case class Purchase(invoiceNo : String, quantity : Int, invoiceDate : String,
                      unitPrice : Double, customerID : String, country : String)

  case class Invoice(invoiceNo : String, avgUnitPrice : Double,
                     minUnitPrice : Double, maxUnitPrice : Double, time : Double,
                     numberItems : Double, lastUpdated : Long, lines : Int, customerId : String)

  final val KAFKA_TOPIC_INVALID           = "facturas_erroneas"
  final val KAFKA_TOPIC_CANCELLED         = "cancelaciones"
  final val KAFKA_TOPIC_ANOMALIES         = "anomalias_kmeans"
  final val KAFKA_TOPIC_ANOMALIES_BISECT  = "anomalias_bisect_kmeans"

  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))

    // TODO: Change to INFO
//    ssc.sparkContext.setLogLevel("INFO")
    ssc.sparkContext.setLogLevel("ERROR")

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // Load model and broadcast
    val (kmeansmodel, threshold) = loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    val (bisectingkmeansModel, bisectingThreshold) = loadBisectingKMeansAndThreshold(sc, modelFileBisect, thresholdFileBisect)

    val broadcast = sc.broadcast(brokers)

    // Build pipeline

    // connect to kafka
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    // Invalid invoices
    def isInvalidInvoice(x: (String, String)): Boolean = {
      x._2.split(",").contains("") || // Empty fields
      x._2.split(",").length != 8     // Incorrect values length
    }
    val invalidInvoices = purchasesFeed.filter(x => isInvalidInvoice(x))
    invalidInvoices.foreachRDD(rdd => if (!rdd.isEmpty()) publishToKafka(KAFKA_TOPIC_INVALID)(broadcast)(rdd))

    // Cancelled invoices
    def isCancelledInvoice(x: (String, String)): Boolean = {
      x._1.startsWith("C")
    }
    val cancelledInvoices = purchasesFeed
      .filter(x => isCancelledInvoice(x))
      .map(x => (x._1, 1))
      .reduceByKey((_, _) => 1)
    val recentCancelledInvoicesAmount = cancelledInvoices.map(_ => ("cancelled", 1)).reduceByKeyAndWindow(
      _ + _,
      _ - _,
      Minutes(8),
      Minutes(1)
    ).map {
      case (key, value) => (key, value.toString)
    }
    recentCancelledInvoicesAmount.foreachRDD(rdd => publishToKafka(KAFKA_TOPIC_CANCELLED)(broadcast)(rdd))

    // Invoices to cluster

    // Convert data to Purchase
    def parsePurchase(data: (String,String)) : (String, Purchase) = {
      val key = data._1
      val Array(invoiceNo, _, _, quantity, invoiceDate, unitPrice, customerId, country) = data._2.split(",")

      (key, Purchase(invoiceNo, quantity.toInt, invoiceDate, unitPrice.toDouble, customerId, country))
    }

    // Update Invoice using new Purchases
    def updateInvoiceState(newPurchases: Seq[Purchase], state: Option[Invoice]): Option[Invoice] = {
      val previous = state.getOrElse(
          // Dummy invoice
          Invoice("XXX", 0.0, Double.MaxValue, Double.MinPositiveValue, 0.0, 0.0, 0L, 0, "XXX")
      )

      if (newPurchases.isEmpty) return None

      var average = previous.avgUnitPrice
      var minimum = previous.minUnitPrice
      var maximum = previous.maxUnitPrice
      val Array(hours, minutes) = newPurchases.head.invoiceDate.split(" ")(1).split(":")
      val hourDouble = hours.toInt + minutes.toInt / 60.0
      var numberItems = previous.numberItems

      // Update values with new Purchases data
      newPurchases.foreach(purchase => {
        average = (average * numberItems + purchase.unitPrice * purchase.quantity)/(numberItems + purchase.quantity)
        minimum = Math.min(minimum,purchase.unitPrice)
        maximum = Math.max(maximum,purchase.unitPrice)
        numberItems = numberItems + purchase.quantity
      })

      // Create Invoice with updated data
      val invoice = Invoice(
          newPurchases.head.invoiceNo,
          average,
          minimum,
          maximum,
          hourDouble,
          numberItems,
          System.currentTimeMillis(),
          previous.lines + 1,
          newPurchases.head.customerID
      )

      Some(invoice)
    }

    // Get Stream with only good invoices
    val goodInvoices = purchasesFeed
      .filter(x => !isInvalidInvoice(x) && !isCancelledInvoice(x))
      .map(x => parsePurchase(x))
      .updateStateByKey(updateInvoiceState)

    // KMeans anomalies
    val anomalies = goodInvoices
      .filter(x => isAnomaly(kmeansmodel, x._2, threshold))
      .map { case (key, value) => (key, value.toString) }
    anomalies.foreachRDD(rdd => publishToKafka(KAFKA_TOPIC_ANOMALIES)(broadcast)(rdd))

    // BisectingKMeans anomalies
    val anomaliesBisect = goodInvoices
      .filter(x => isAnomaly(bisectingkmeansModel, x._2, bisectingThreshold))
      .map { case (key, value) => (key, value.toString) }
    anomaliesBisect.foreachRDD(rdd => publishToKafka(KAFKA_TOPIC_ANOMALIES_BISECT)(broadcast)(rdd))

    // Rest of the pipeline

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def publishToKafka(topic : String)(kafkaBrokers : Broadcast[String])(rdd : RDD[(String, String)]) = {
    rdd.foreachPartition( partition => {
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach( record => {
        producer.send(new ProducerRecord[String, String](topic, record._1,  record._2.toString))
      })
      producer.close()
    })
  }

  def kafkaConf(brokers : String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
   * Load the model information: centroid and threshold
   */
  def loadKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[KMeansModel,Double] = {
    val kmeans = KMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (kmeans, threshold)
  }

  /**
   * Load the model information: centroid and threshold
   */
  def loadBisectingKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[BisectingKMeansModel,Double] = {
    val bisectkmeans = BisectingKMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (bisectkmeans, threshold)
  }

  def connectToPurchases(ssc: StreamingContext, zkQuorum : String, group : String,
                         topics : String, numThreads : String): DStream[(String, String)] ={

    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

  def invoiceToVector(invoice: Invoice): Vector = {
    Vectors.dense(
      invoice.avgUnitPrice,
      invoice.minUnitPrice,
      invoice.maxUnitPrice,
      invoice.time,
      invoice.numberItems
    )
  }

  def isAnomaly(model: KMeansModel, invoice: Invoice, threshold: Double): Boolean = {
    // Anomaly if distance > threshold
    distToCentroid(invoiceToVector(invoice), model) > threshold
  }

  // From train.scala
  def distToCentroid(datum: Vector, model: KMeansModel) : Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

  def isAnomaly(model: BisectingKMeansModel, invoice: Invoice, threshold: Double): Boolean = {
    // Anomaly if distance > threshold
    distToCentroid(invoiceToVector(invoice), model) > threshold
  }

  // From trainBisecting.scala
  def distToCentroid(datum: Vector, model: BisectingKMeansModel) : Double = {
    val centroid = model.clusterCenters(model.predict(datum)) // if more than 1 center
    Vectors.sqdist(datum, centroid)
  }

}
