package com.scalapractice

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object PopularMoviewNicer {
  
  def loadMovieNames() : Map[Int, String] = {
    
    //Handle character encoding issues
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    
    //Create a Map of Ints to Strings, and populate it from u.item
    var moviewNames:Map[Int, String] = Map()
    
    val lines = Source.fromFile("./data/u.item").getLines()
    for(line <- lines) {
      var fields = line.split('|')
      if(fields.length > 1){
        moviewNames += (fields(0).toInt -> fields(1))
      }
     }
    return moviewNames
  }
  
 
  def main(args:Array[String]){
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    //create a spark Context using every core of the local machine
    val sc = new SparkContext("local[*]" , "PopularMovies");
    
     // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
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
    val sortedMovies = flipped.sortByKey()
    
    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map( x  => (nameDict.value(x._2), x._1) )
    
    // Collect and print results
    val colletRating = sortedMoviesWithNames.collect();
    
    colletRating.foreach(println);
    
  }
}