package app
import java.io

import org.apache.spark.ml.classification.LogisticRegression
import bean.{Accident, AccidentCount, Casualty, Vehicle}
import dao.AccidentDAO
import data.{DataCleaning, DataSetCreation}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{DataSetGenerator, HBaseUtils, SparkSessionFactory}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import utils.DataSetGenerator.{accidentDS, casualtyDS, spark, vehicleDS}

object App {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop");
// Zookeeper host:port, group, kafka topic, threads
//    if (args.length != 4) {
//      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
//      System.exit(1)
//    }
// spark sql initialization
// streaming preparation
// val Array(zkQuorum, groupId, topics, numThreads) = args
// val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
// val messages = KafkaUtils.createStream(streamingContext, zkQuorum, groupId, topicMap)
// Streaming Data
    val spark = SparkSessionFactory.getSparkSession
    import spark.implicits._
    val streamingContext = SparkSessionFactory.getStreamingContext
    DataSetGenerator.init
    var accidentDS: Dataset[Accident] = DataSetGenerator.accidentDS
    var vehicleDS: Dataset[Vehicle] = DataSetGenerator.vehicleDS
    var casualtyDS = DataSetGenerator.casualtyDS
    var joinedTable_train: Dataset[Row] = DataSetGenerator.trainSetJoinedGen
    var joinedTable_test: Dataset[Row] = DataSetGenerator.testSetJoinedGen
    var accident_train: Dataset[Accident] = DataSetGenerator.trainSetAccidentGen
    var accident_test: Dataset[Accident] = DataSetGenerator.testSetAccidentGen
    val streamingData: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop000", 9999)
    val input: DStream[Serializable] = streamingData.map (data => DataCleaning.parseData(data))
    var count = 0
    input.foreachRDD(
      strs => {
        val data: Array[Serializable] = strs.collect()
        data.foreach(d => d match {
          case casualty: Casualty => {
            println(casualty)
            count = count+1
            casualtyDS = casualtyDS.union(Seq(casualty).toDS())
          }
          case accident: Accident => {
            println(accident)
            count = count+1
            accidentDS = accidentDS.union(Seq(accident).toDS())
          }
          case vehicle: Vehicle => {
            println(vehicle)
            count = count+1
            vehicleDS = vehicleDS.union(Seq(vehicle).toDS())
            accidentDS.createOrReplaceGlobalTempView("accident")
            val sql = "select accident_index, accident_severity from accident"
            val accident_serverity: DataFrame = spark.sql(sql)
            joinedTable_train = accident_serverity.join(vehicleDS,"accident_index").join(casualtyDS,"accident_index")
            println(accident_serverity.show(1))
            println("Data Added")
          }
          case _ => println("Data Parsing Failed")
        })}
    )
    streamingContext.start()
    streamingContext.awaitTermination()
  }



















//  def linearregressor = {
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)
//
//    val numFolds = 10
//    val MaxIter: Seq[Int] = Seq(1000)
//    val RegParam: Seq[Double] = Seq(0.001)
//    val Tol: Seq[Double] = Seq(1e-6)
//    val ElasticNetParam: Seq[Double] = Seq(0.001)
//
//    // Create an LinerRegression estimator
//    val model = new LinearRegression().setFeaturesCol("features").setLabelCol("label")
//
//    // Building the Pipeline for transformations and predictor
//    println("Building ML pipeline")
//    val pipeline = new Pipeline().setStages((Preproessing.stringIndexerStages :+ Preproessing.assembler) :+ model)
//
//    // ***********************************************************
//    println("Preparing K-fold Cross Validation and Grid Search: Model tuning")
//    // ***********************************************************
//    val paramGrid = new ParamGridBuilder()
//      .addGrid(model.maxIter, MaxIter)
//      .addGrid(model.regParam, RegParam)
//      .addGrid(model.tol, Tol)
//      .addGrid(model.elasticNetParam, ElasticNetParam)
//      .build()
//
//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(new RegressionEvaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(numFolds)
//
//    // ************************************************************
//    println("Training model with Linear Regression algorithm")
//    // ************************************************************
//    val cvModel = cv.fit(Preproessing.trainingData)
//    //val cvModel2 = cv.fit(Preproessing.testData)
//
//    // Save the workflow
//    cvModel.write.overwrite().save("model/LR_model")
//
//    // Load the workflow back
//    val sameCV = CrossValidatorModel.load("model/LR_model")
//
//    // **********************************************************************
//    println("Evaluating model on train and validation set and calculating RMSE")
//    // **********************************************************************
//    val trainPredictionsAndLabels = cvModel.transform(Preproessing.trainingData).select("label", "prediction")
//      .map { case Row(label: Double, prediction: Double) => (label, prediction) }.rdd
//
//    val validPredictionsAndLabels = cvModel.transform(Preproessing.validationData).select("label", "prediction")
//      .map { case Row(label: Double, prediction: Double) => (label, prediction) }.rdd
//
//    val trainRegressionMetrics = new RegressionMetrics(trainPredictionsAndLabels)
//    val validRegressionMetrics = new RegressionMetrics(validPredictionsAndLabels)
//    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
//
//    val results = "\n=====================================================================\n" +
//      s"Param trainSample: ${DataProessing.trainSample}\n" +
//      s"Param testSample: ${DataProessing.testSample}\n" +
//      s"TrainingData count: ${DataProessing.trainingData.count}\n" +
//      s"ValidationData count: ${DataProessing.validationData.count}\n" +
//      s"TestData count: ${DataProessing.testData.count}\n" +
//      "=====================================================================\n" +
//      s"Param maxIter = ${MaxIter.mkString(",")}\n" +
//      s"Param numFolds = ${numFolds}\n" +
//      "=====================================================================\n" +
//      s"Training data MSE = ${trainRegressionMetrics.meanSquaredError}\n" +
//      s"Training data RMSE = ${trainRegressionMetrics.rootMeanSquaredError}\n" +
//      s"Training data R-squared = ${trainRegressionMetrics.r2}\n" +
//      s"Training data MAE = ${trainRegressionMetrics.meanAbsoluteError}\n" +
//      s"Training data Explained variance = ${trainRegressionMetrics.explainedVariance}\n" +
//      "=====================================================================\n" +
//      s"Validation data MSE = ${validRegressionMetrics.meanSquaredError}\n" +
//      s"Validation data RMSE = ${validRegressionMetrics.rootMeanSquaredError}\n" +
//      s"Validation data R-squared = ${validRegressionMetrics.r2}\n" +
//      s"Validation data MAE = ${validRegressionMetrics.meanAbsoluteError}\n" +
//      s"Validation data Explained variance = ${validRegressionMetrics.explainedVariance}\n" +
//      s"CV params explained: ${cvModel.explainParams}\n" +
//      s"GBT params explained: ${bestModel.stages.last.asInstanceOf[LinearRegressionModel].explainParams}\n" +
//      "=====================================================================\n"
//    println(results)
//
//    // *****************************************
//    println("Run prediction on the test set")
//    cvModel.transform(Preproessing.testData)
//      .select("id", "prediction")
//      .withColumnRenamed("prediction", "loss")
//      .coalesce(1)
//      .write.format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("output/result_LR.csv")
//
//    spark.stop()
//  }
}
