package com.scalapractice

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object FriendsAge {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "FriendsAge")
   
    val fakeFriends = sc.textFile("./data/fakefriends.csv");
    val rdd = fakeFriends.map(parseLine);
    //(33,385)
    //(33,2)
    //(55,221)
    
    val totalByAge = rdd.mapValues((x) => (x,1)).reduceByKey((x,y) => (x._1+y._1, x._2+y._2));
    totalByAge.sortByKey(false, 1);
   
    totalByAge.foreach(println)
    
    val avgByAge = totalByAge.mapValues(x=> x._1 / x._2).collect().sorted
    avgByAge.foreach(println)
    
    
    //rdd.mapValues((x)=>(x,1))
    //(33,385) => (33, (385,1))
    //(33,2) => (33, (2, 1))
    //(55,221) => (55, 221, 1))
    
    //reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
     //(33,385) => (33, (387,2))
    
  }
  def parseLine(line: String) = {
    val fields = line.split(",");
    val age = fields(2).toInt;
    val numFriends = fields(3).toInt
    (age,numFriends)
  }
}
