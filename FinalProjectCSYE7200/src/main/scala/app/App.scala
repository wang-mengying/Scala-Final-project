package app

import bean.{Accident, AccidentCount, Casualty, Vehicle}
import dao.AccidentDAO
import data.DataSetCreation
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.HBaseUtils

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{ Pipeline, PipelineModel }
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.RegressionMetrics

import org.apache.log4j.Logger
import org.apache.log4j.Level

object App {
  def main(args: Array[String]): Unit = {

    // Zookeeper host:port, group, kafka topic, threads
    if (args.length != 4) {
      println("Usage: StatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    // spark sql initialization
    val conf: SparkConf = new SparkConf().setAppName("CSYE7200FinalProject").set("spark.storage.memoryFraction","0.6").set("spark.shuffle.file.buffer","64")
      .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
    conf.registerKryoClasses(Array(classOf[Accident],classOf[Casualty],classOf[Vehicle],classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // Data from local system
    val vehicleDS:Dataset[Vehicle] = DataSetCreation.getVehicleData("in/Vehicles.csv",spark)
    val accidentDS:Dataset[Accident] = DataSetCreation.getAccidentData("in/Accidents.csv",spark)
    val casualtyDS:Dataset[Casualty] = DataSetCreation.getCasualtyData("in/Casualties.csv",spark)
    vehicleDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
    accidentDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
    casualtyDS.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // streaming preparation
    val Array(zkQuorum, groupId, topics, numThreads) = args
    val streamingContext = new StreamingContext(conf,Seconds(30))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val messages = KafkaUtils.createStream(streamingContext, zkQuorum, groupId, topicMap)
    // etl
    messages.map(_._2).map {
      d => {
        val data: Array[String] = d.split(",")
        d.split(",").length match {
          case 16 => {
            val ds: Dataset[Casualty] = Seq(Casualty(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15))).toDS()
            casualtyDS.union(ds)
          }
          case 23 => {
            val ds: Dataset[Vehicle] = Seq(Vehicle(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15), data(16), data(17), data(18), data(19), data(20), data(21), data(22))).toDS()
            vehicleDS.union(ds)
          }
          case 31 => {
            val ds: Dataset[Accident] = Seq(Accident(data(0), data(1), data(2), data(3), data(4), data(5), data(6), data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14), data(15), data(16), data(17), data(18), data(19), data(20), data(21), data(22), data(23), data(24), data(25), data(26), data(27), data(28), data(29), data(30))).toDS()
            accidentDS.union(ds)
            AccidentDAO.save(AccidentCount(data(16)+"_"+data(17),1))
          }
          case _ => println("Please check your data")
        }
      }
    }
//    casualtyDS.show(5)
//    vehicleDS.show(5)
//    accidentDS.show(5)

    // machine learning algo
    machineLearning

    streamingContext.start()
    streamingContext.awaitTermination()
  }
  def linearregressor = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val numFolds = 10
    val MaxIter: Seq[Int] = Seq(1000)
    val RegParam: Seq[Double] = Seq(0.001)
    val Tol: Seq[Double] = Seq(1e-6)
    val ElasticNetParam: Seq[Double] = Seq(0.001)

    // Create an LinerRegression estimator
    val model = new LinearRegression().setFeaturesCol("features").setLabelCol("label")

    // Building the Pipeline for transformations and predictor
    println("Building ML pipeline")
    val pipeline = new Pipeline().setStages((Preproessing.stringIndexerStages :+ Preproessing.assembler) :+ model)

    // ***********************************************************
    println("Preparing K-fold Cross Validation and Grid Search: Model tuning")
    // ***********************************************************
    val paramGrid = new ParamGridBuilder()
      .addGrid(model.maxIter, MaxIter)
      .addGrid(model.regParam, RegParam)
      .addGrid(model.tol, Tol)
      .addGrid(model.elasticNetParam, ElasticNetParam)
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)

    // ************************************************************
    println("Training model with Linear Regression algorithm")
    // ************************************************************
    val cvModel = cv.fit(Preproessing.trainingData)
    //val cvModel2 = cv.fit(Preproessing.testData)

    // Save the workflow
    cvModel.write.overwrite().save("model/LR_model")

    // Load the workflow back
    val sameCV = CrossValidatorModel.load("model/LR_model")

    // **********************************************************************
    println("Evaluating model on train and validation set and calculating RMSE")
    // **********************************************************************
    val trainPredictionsAndLabels = cvModel.transform(Preproessing.trainingData).select("label", "prediction")
      .map { case Row(label: Double, prediction: Double) => (label, prediction) }.rdd

    val validPredictionsAndLabels = cvModel.transform(Preproessing.validationData).select("label", "prediction")
      .map { case Row(label: Double, prediction: Double) => (label, prediction) }.rdd

    val trainRegressionMetrics = new RegressionMetrics(trainPredictionsAndLabels)
    val validRegressionMetrics = new RegressionMetrics(validPredictionsAndLabels)
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]

    val results = "\n=====================================================================\n" +
      s"Param trainSample: ${DataProessing.trainSample}\n" +
      s"Param testSample: ${DataProessing.testSample}\n" +
      s"TrainingData count: ${DataProessing.trainingData.count}\n" +
      s"ValidationData count: ${DataProessing.validationData.count}\n" +
      s"TestData count: ${DataProessing.testData.count}\n" +
      "=====================================================================\n" +
      s"Param maxIter = ${MaxIter.mkString(",")}\n" +
      s"Param numFolds = ${numFolds}\n" +
      "=====================================================================\n" +
      s"Training data MSE = ${trainRegressionMetrics.meanSquaredError}\n" +
      s"Training data RMSE = ${trainRegressionMetrics.rootMeanSquaredError}\n" +
      s"Training data R-squared = ${trainRegressionMetrics.r2}\n" +
      s"Training data MAE = ${trainRegressionMetrics.meanAbsoluteError}\n" +
      s"Training data Explained variance = ${trainRegressionMetrics.explainedVariance}\n" +
      "=====================================================================\n" +
      s"Validation data MSE = ${validRegressionMetrics.meanSquaredError}\n" +
      s"Validation data RMSE = ${validRegressionMetrics.rootMeanSquaredError}\n" +
      s"Validation data R-squared = ${validRegressionMetrics.r2}\n" +
      s"Validation data MAE = ${validRegressionMetrics.meanAbsoluteError}\n" +
      s"Validation data Explained variance = ${validRegressionMetrics.explainedVariance}\n" +
      s"CV params explained: ${cvModel.explainParams}\n" +
      s"GBT params explained: ${bestModel.stages.last.asInstanceOf[LinearRegressionModel].explainParams}\n" +
      "=====================================================================\n"
    println(results)

    // *****************************************
    println("Run prediction on the test set")
    cvModel.transform(Preproessing.testData)
      .select("id", "prediction")
      .withColumnRenamed("prediction", "loss")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("output/result_LR.csv")

    spark.stop()
  }
}
