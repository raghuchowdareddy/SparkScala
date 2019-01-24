package com.scalapractice

import org.apache.spark.SparkContext
import org.apache.log4j._

object WordCountBetter {
  
  def main(args: Array[String]) {
    
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]" , "WordCountBetter");
    val lines = sc.textFile("./data/book.txt")
    
    //print word count better using regular expression
      val word_better = lines.flatMap(x => x.split("\\W+"))
      
      //Normalize everything to lowercase
      val lowercase = word_better.map(x=>x.toLowerCase());
      
      // count words
      val wordcounts = lowercase.map(x => (x,1)).reduceByKey((x,y) => x+y)
      
      val wordCountSorted = wordcounts.map(x=> (x._2,x._1)).sortByKey() // flip the key ,value from (word,count) => (count, word) to sort it easily
      
      for(result <- wordCountSorted){
        val count = result._1;
        val word = result._2;
        println(s"$word : $count")
      }
    
  }
}