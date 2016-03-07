package org.test.spark

import tokenize.Tokenizer
import java.io._
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BigramCount extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    log.info("Input: " + "pg100.txt")
    log.info("Output: " + "BigramCount.txt")
    log.info("Number of reducers: " + "1")
   
    val conf = new SparkConf()
        .setMaster("local")
        .setAppName("bigram analysis")
    val sc = new SparkContext(conf)

    val outputDir = new Path("BigramCount.txt")
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile("pg100.txt")
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1){
          tokens.sliding(2).map(p => p.mkString(" ")).toList
        }
        else List()
      })
    val count = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1){
          tokens.map(p => p + " *").toList  
        }
        else List()
      })
    val rddall = counts.union(count)
    val mapping = rddall.map(partial => (partial, 1))
    
    val reducing = mapping.reduceByKey(_+_)
    val sort = reducing.sortByKey()
    var marginal:Float=1.0f
    val relativefreq = sort
    .map(k =>
      if(k._1.split(" ")(1)== "*"){
        marginal = k._2.toFloat
        (k._1,k._2)
      }
      else{
        (k._1,k._2/marginal)
      }
      )
    relativefreq.saveAsTextFile("Bigram")
    
  }
}