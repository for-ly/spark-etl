package com.corp.car.etl.filter

trait IFilter extends Serializable{
    def filter(p: Any,config: Any):Boolean
}
