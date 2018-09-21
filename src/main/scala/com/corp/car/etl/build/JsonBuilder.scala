package com.corp.car.etl.build

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import com.alibaba.fastjson.JSON
import com.corp.car.etl.bean.{ColumnModel, JsonConfig}
import com.corp.mobile.car.common.log.UnifyLogger

import scala.collection.{JavaConversions, mutable}
import scala.io.Source

class JsonBuilder extends Serializable {

  val analysis_mark = "->"

  val replace_mark = mutable.LinkedHashMap("\"\"" -> "\"","\\\\\\\"" -> "\\\"","\\" -> "", "\"{" -> "{","}\"" -> "}")

  var fileColumn = scala.collection.mutable.ArrayBuffer.newBuilder[ColumnModel]

  var targetColumn = scala.collection.mutable.ArrayBuffer.newBuilder[String]

  var analysisTargetColumns = scala.collection.mutable.ArrayBuffer.newBuilder[java.util.Map[String, java.util.Map[String, String]]]

  var finalColumns = scala.collection.mutable.ArrayBuffer.newBuilder[ColumnModel]

  def init(conf: JsonConfig): Unit = {
    fileColumn = fileColumn.result().++((JavaConversions.asScalaBuffer(conf.getFileColumn)))
    targetColumn = targetColumn.result().++(JavaConversions.asScalaBuffer(conf.getTargetColumn))
    analysisTargetColumns = analysisTargetColumns.result().++(JavaConversions.asScalaBuffer(conf.getAnalysisTargetColumns).asInstanceOf[scala.collection.mutable.Buffer[java.util.Map[String, java.util.Map[String, String]]]])

    this.fileColumn.result().map(ori => {
      this.targetColumn.result().map(target => {
        if (target.equals(ori.getName))
          finalColumns.result().append(ori)
      })
    })

    this.analysisTargetColumns.result().map(atc => {
      atc.keySet().toArray.map(key => {
        val contentMap = JavaConversions.mapAsScalaMap(atc.get(key))
        for ((k, v) <- contentMap) {
          val columnModel = new ColumnModel()
          columnModel.setName(k)
          columnModel.setType("string")
          finalColumns.result().append(columnModel)
        }
      })
    })
  }

  def getJsonConfig(json: String): JsonConfig = {
    val clazz: java.lang.Class[_] = Class.forName("com.corp.car.etl.bean.JsonConfig")
    val jsonConfig = JSON.parseObject(json, clazz).asInstanceOf[JsonConfig]
    init(jsonConfig)
    check()
    jsonConfig
  }

  def check(): Unit = {
    val names: Seq[String] = fileColumn.result().map(_.getName)
    for (target <- targetColumn.result()) {
      if (!names.contains(target)) {
        UnifyLogger.error("JsonBuilder","check",false,"生成列不在配置的文件列中!!!")
        throw new RuntimeException("生成列不在配置的文件列中!!!")
      }
    }
    val keys = scala.List.newBuilder
    for (analysis_target <- analysisTargetColumns.result()) {
      JavaConversions.mapAsScalaMap(analysis_target.asInstanceOf[java.util.Map[String, java.util.Map[String, String]]])
        .keySet.map(key => keys.+(key.toString))
    }
    for (key <- keys.result()) {
      if (!names.contains(key)) {
        UnifyLogger.error("JsonBuilder","check",false,"分析列不在配置的文件列中!!!")
        throw new RuntimeException("分析列不在配置的文件列中!!!")
      }
    }
  }

  def getSchema(): Seq[ColumnModel] = {
    finalColumns.result()
  }

}

object JsonBuilder {

  val builder = new JsonBuilder

  def getLocalJsonConfig(path: String): String = {
    val json = new StringBuffer()
    val localfile = Source.fromFile(path)
    try {
      for (line <- localfile.getLines())
        json.append(line)
      json.toString
    } catch {
      case e: Exception => {
        println(e.getMessage)
        throw new Exception
      }
    } finally {
      localfile.close()
    }
  }

  def getHdfsJsonConfig(path: String): String = {
    readLinesFromHDFS(path)
  }

  def readLinesFromHDFS(path: String): String = {
    val pt = new Path(path)
    val br: BufferedReader = new BufferedReader(new InputStreamReader(FileSystem.get(new Configuration).open(pt)))
    val lines = new StringBuffer
    var line = br.readLine()
    while (line != null) {
      lines.append(line)
      line = br.readLine()
    }
    lines.toString
  }
}
