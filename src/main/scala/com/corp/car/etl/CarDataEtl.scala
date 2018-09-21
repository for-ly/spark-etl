package com.corp.car.etl

import com.corp.car.etl.bean.JsonConfig
import com.corp.car.etl.filter.CFilter
import com.corp.car.etl.process.Processor
import com.corp.car.etl.build.{DataBuilder, JsonBuilder}
import com.corp.mobile.car.common.log.UnifyLogger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}


object CarDataEtl {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val ss = SparkSession.builder().config(conf).appName("car_data_etl")
      //      .master("local")
      .getOrCreate()
    val etlConf = EtlConfig(conf)
    try {
      val jc = JsonBuilder.builder.getJsonConfig(JsonBuilder.getHdfsJsonConfig(etlConf.config))
      val paras = ss.sparkContext.broadcast((jc, DataBuilder.buildSchema(JsonBuilder.builder.getSchema()), JsonBuilder.builder))
      val rdd = ss.sparkContext.textFile(etlConf.input).filter(line => CFilter.filter(line, paras.value._1))
        .mapPartitions(run(_, paras.value._1, paras.value._3)).repartition(etlConf.partNum.toString.toInt)
      ss.sqlContext.createDataFrame(rdd, paras.value._2).write.format("parquet").save(etlConf.output)
    } catch {
      case e: Exception => {
        UnifyLogger.error("CarDataEtl","main",false,e.getMessage)
        throw new Exception(e)
      }
    } finally {
      ss.stop()
    }
  }

  def run(iterator: Iterator[String], cc: JsonConfig, builder: JsonBuilder): Iterator[Row] = {
    val partVal = iterator.map(line => Row.fromSeq(Processor.parserLine(line, cc, builder)))
      .filter(row => if (row.length == builder.getSchema().length) true
       else {
        UnifyLogger.info("CarDataEtl.run", false, row.mkString("##"))
        false
      }
      )
    partVal
  }
}