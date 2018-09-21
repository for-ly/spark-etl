package com.corp.car.etl.filter

import com.corp.car.etl.bean.JsonConfig

class CommonFilter extends IFilter {
  override def filter(line: Any, config: Any): Boolean = {
    if (line.isInstanceOf[String] && config.isInstanceOf[JsonConfig]) {
      isFilter(line.asInstanceOf[String], config.asInstanceOf[JsonConfig]) match {
        case true => return true
        case _ => return false
      }
    }
    false
  }

  def isFilter(line: String, config: JsonConfig): Boolean = {
    val tags = config.getFilterTag
    var flag = true
    for (tag <- tags.values().toArray()) {
      flag = flag && line.contains(tag.toString)
    }
    flag
  }

}

object CFilter {
  val commonFilter = new CommonFilter()
  def filter(p: Any, config: Any): Boolean = {
    commonFilter.filter(p, config)
  }
}
