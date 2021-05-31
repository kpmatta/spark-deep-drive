package com.example.spark

//import java.util.logging.{Level, Logger}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession

trait SparkSessionLocal {
 lazy val spark: SparkSession = SparkSession.builder()
   .appName("Test")
   .master("local[*]")
   .getOrCreate()

 Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
 Logger.getLogger("org.spark-project").setLevel(Level.WARN)
}
