package com.scalapractice

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.optimization.SquaredL2Updater

object LinearRegression {
  
   def main(args:Array[String]){
     
     val sc = new SparkContext("local[*]", "LinearRegression")
     
     val trainingLines = sc.textFile("./data/regression.txt")
     
     val testLines = sc.textFile("./data/regression.txt")
     
     //convert input data to LablePoints for MLLib
     
     val trainingData = trainingLines.map(LabeledPoint.parse).cache();
     val testData = testLines.map(LabeledPoint.parse)
     
     //now create our linear regression model
     
     val algorithm = new LinearRegressionWithSGD()
     algorithm.
     optimizer
     .setNumIterations(100)
     .setStepSize(1.0)
     .setUpdater(new SquaredL2Updater())
     .setRegParam(0.01)
     
     val model = algorithm.run(trainingData)
     
     //Predict values of our test feature data using our linear regression model
     val predications = model.predict(testData.map(_.features))
     
     //zip it so that we can compare it later
     val predictionAndModel = predications.zip(testData.map(_.label))
     
     //print out the predicted and actual values for each point
     for(prediction <- predictionAndModel){
       println(prediction)
     }
     
     
     
     
   }
}