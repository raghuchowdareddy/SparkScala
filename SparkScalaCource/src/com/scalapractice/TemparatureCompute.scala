package com.scalapractice

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import breeze.linalg.min

object TemparatureCompute {
  
  def main(args: Array[String]){
    
     val sc = new SparkContext("local[*]", "TemparatureCompute")
   
    val lines = sc.textFile("./data/1800.csv");
    val parsedLines = lines.map(parseLine)
    //parsedLines.foreach(println)
    
    val allMinTemps = parsedLines.filter((x) => x._2=="TMIN");
    val stationTemps = allMinTemps.map(x => (x._1 , x._3.toFloat));
    val mintemp = stationTemps.reduceByKey((x,y) => min(x,y))
    mintemp.foreach(println)
    
  }
  def parseLine(line:String)  = {
    val fields = line.split(",");
    val stationID = fields(0);
    val entryType = fields(2);
    val temparature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32f
    (stationID, entryType, temparature)
  }
}