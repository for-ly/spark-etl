package com.corp.car.etl.build

import com.corp.car.etl.bean.{ColumnModel, ColumnResult}
import org.apache.spark.sql.types.{DataTypes, StructType}

class DataBuilder extends Serializable {

}

object DataBuilder {
  def buildSchema(schema: Seq[ColumnModel]): StructType = {
    var struct = new StructType()
    for (model <- schema) {
      struct = model.getType.toLowerCase match {
        case "string" => struct.add(model.getName, DataTypes.StringType)
        case "int" => struct.add(model.getName, DataTypes.IntegerType)
        case "double" => struct.add(model.getName, DataTypes.DoubleType)
        case "timestamp" => struct.add(model.getName, DataTypes.StringType)
        case "long" => struct.add(model.getName, DataTypes.LongType)
        case "float" => struct.add(model.getName, DataTypes.FloatType)
        case "short" => struct.add(model.getName, DataTypes.ShortType)
        case "date" => struct.add(model.getName, DataTypes.StringType)
      }
    }
    struct
  }

  def buildData(columnModel: ColumnResult): Any = {
    val form = columnModel.getType.toLowerCase match {
      case "string" => {
        if (columnModel.getValue == null)
          "-"
        else
          columnModel.getValue.toString
      }
      case "int" => {
        if (columnModel.getValue == null)
          -999
        else
          columnModel.getValue.toString.toInt
      }
      case "double" => {
        if (columnModel.getValue == null)
          -999
        else
          columnModel.getValue.toString.toDouble
      }
      case "float" => {
        if (columnModel.getValue == null)
          -999
        else
          columnModel.getValue.toString.toFloat
      }
      case "short" => {
        if (columnModel.getValue == null)
          -999
        else
          columnModel.getValue.toString.toShort
      }
      case "timestamp" => {
        if (columnModel.getValue == null)
          "883584000" //1998-01-01 00:00:00
        else
          columnModel.getValue.toString
      }
      case "date" => {
        if (columnModel.getValue == null)
          "1998-01-01" //1998-01-01
        else
          columnModel.getValue.toString
      }
    }

    form
  }
}
