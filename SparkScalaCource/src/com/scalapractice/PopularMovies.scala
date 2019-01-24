package com.scalapractice

import org.apache.log4j._
import org.apache.spark.SparkContext

object PopularMovies {
  
  def main(args:Array[String]){
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //create a spark Context using every core of the local machine
    val sc = new SparkContext("local[*]" , "PopularMovies");
    
    val lines  = sc.textFile("./data/u.data");
    
    //sample data
    //userId MovieId Rating Timestamp
    //196	   242	    3	     881250949
    //186	   302	    3	     891717742
    
    //map to (MoviewId, 1) tuples
    val movies = lines.map(x=> (x.split("\t")(1).toInt , 1))
    
    //count of all 1's in each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    
    //flip (movieId, count) to (count, movieId)
    val flipped = movieCounts.map(x=> (x._2 , x._1))
    
    //get it sorted
    val sorted = flipped.sortByKey()
    
    // Collect and print results
    val colletRating = sorted.collect();
    
    colletRating.foreach(println);
    
  }
  
}