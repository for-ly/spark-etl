package com.corp.car.etl

import org.apache.spark.SparkConf

case class EtlConfig(conf: SparkConf) {
  val input = conf.getOption("spark.conf.input").getOrElse("D:\\part-00062\\part-00062")
  val config = conf.getOption("spark.conf.config").getOrElse("D:\\json_config_demo.json")
  val output = conf.getOption("spark.conf.output").getOrElse("D:\\file")
  val partNum = conf.getOption("spark.conf.partNum").getOrElse(24)
}
