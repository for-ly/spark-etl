package com.corp.car.etl.process

import com.alibaba.fastjson.JSON
import com.corp.car.etl.bean.{ColumnResult, JsonConfig}
import com.corp.car.etl.build.{DataBuilder, JsonBuilder}
import com.corp.mobile.car.common.log.UnifyLogger

import util.control.Breaks._
import scala.collection.JavaConversions

class Processor extends Serializable {
  def apply(line: String, config: JsonConfig, builder: JsonBuilder): Seq[Any] = {
    try {
      val result = new java.util.ArrayList[ColumnResult]()
      val targetColumn1 = getTargetColumn(line, config.getFileSplitTag, builder)
      val targetColumn2 = getAnalysisTargetColumnValue(line, config.getFileSplitTag, builder)
      targetColumn1.addAll(targetColumn2)
      //按照schema的列顺序排列
      for (schema <- builder.getSchema()) {
        breakable {
          for (column <- JavaConversions.asScalaBuffer(targetColumn1)) {
            if (schema.getName.toLowerCase.equals(column.getName.toLowerCase)) {
              result.add(column)
              break()
            }
          }
        }
      }
      JavaConversions.asScalaBuffer(result).map(DataBuilder.buildData(_))
    } catch {
      case e: Exception => UnifyLogger.error("Processor","apply",false,e.getMessage)
        throw new RuntimeException(e)
    }
  }

  def getTargetColumn(line: String, split: String, builder: JsonBuilder): java.util.List[ColumnResult] = {
    val result = new java.util.ArrayList[ColumnResult]()
    val lineSegment = line.split(split)
    val targetColumn = builder.targetColumn
    targetColumn.result().map(target => {
      builder.fileColumn.result().foreach(ori => {
        if (ori.getName.equals(target)) {
          val columnResult = new ColumnResult()
          columnResult.setName(target)
          columnResult.setValue(lineSegment(ori.getIndex))
          columnResult.setType(ori.getType)
          result.add(columnResult)
        }
      })
    })
    result
  }

  def getAnalysisTargetColumnValue(line: String, split: String, builder: JsonBuilder): java.util.List[ColumnResult] = {
    val result = new java.util.ArrayList[ColumnResult]()
    val lineSegment = line.split(split)
    builder.analysisTargetColumns.result().map(
      atc => {
        atc.keySet().toArray.map(key => {
          builder.fileColumn.result().map(column => {
            if (column.getName.equals(key)) {
              var correctLine = lineSegment(column.getIndex)
              //转换成json
              for ((k, v) <- builder.replace_mark)
                correctLine = correctLine.replace(k, v)
              val contentMap = JavaConversions.mapAsScalaMap(atc.get(key))
              for ((k, v) <- contentMap) {
                var contentObject = JSON.parseObject(correctLine)
                val analysisSegment = v.toString.split(builder.analysis_mark)
                for (i <- 0 until analysisSegment.length - 1) {
                  contentObject = contentObject.getJSONObject(analysisSegment(i))
                }
                val columnResult = new ColumnResult()
                columnResult.setName(k)
                columnResult.setType("string")
                columnResult.setValue(contentObject.getOrDefault(analysisSegment(analysisSegment.length - 1), "-").toString)
                result.add(columnResult)
              }
            }
          })
        })
      }
    )
    result
  }
}

object Processor {
  val parser = new Processor

  def parserLine(line: String, js: JsonConfig, builder: JsonBuilder): Seq[Any] = {
    parser.apply(line, js, builder)
  }
}
