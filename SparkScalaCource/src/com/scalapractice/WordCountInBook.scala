package com.scalapractice

import org.apache.spark.SparkContext
import org.apache.log4j._

object WordCountInBook {
  
  def main(args: Array[String]) {
    
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]" , "WordCountInBook");
    val lines = sc.textFile("./data/book.txt")
    
    val words = lines.flatMap(x => x.split(" "))
    //words.foreach(println)
    val counts = words.countByValue();
    counts.foreach(println)
    
    //print word count better using regular expression
      val word_better = lines.flatMap(x => x.split("\\W+"))
      
      //Normalize everything to lowercase
      val lowercase = word_better.map(x=>x.toLowerCase());
      
      // count words
      val wordcounts = lowercase.countByValue(); //hard way of doing it. not recommended
      wordcounts.foreach(println);
    
  }
}