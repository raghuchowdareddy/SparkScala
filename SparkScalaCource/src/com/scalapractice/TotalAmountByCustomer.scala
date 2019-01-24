package com.scalapractice

import org.apache.spark.SparkContext
import org.apache.log4j._

object TotalAmountByCustomer {
  
  def main(args: Array[String]){
   // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]" , "WordCountBetter");
    val lines = sc.textFile("./data/customer-orders.csv");
    //Sample
    //44,8602,37.19
    //35,5368,65.89
    //2,3391,40.64
    
    val customer_amount = lines.map(parseLine)
    val totalAmountByCustomer = customer_amount.reduceByKey((x,y) => x+y).collect();
   
    val sortedByAmount =  totalAmountByCustomer.map(x =>(x._2,x._1)).sorted
    
    sortedByAmount.foreach(println);
    
//    for(result <- sortedByAmount){
//      val amount = result._1;
//      val curomerId = result._2;
//      val formattedAmuint = f"$amount%.2f"
//      println(s"$curomerId :$formattedAmuint")
//    }
   
  }
  def parseLine(line: String)={
    val fields = line.split(",");
    val customerId = fields(0).toInt;
    val amount = fields(2).toFloat;
    (customerId, amount)
  }
}