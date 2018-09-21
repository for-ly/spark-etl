package com.corp.car.etl.bean;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JsonConfig implements Serializable {
    private String bussinessType;
    private String fileSplitTag;
    private List<ColumnModel> fileColumn;
    private Map filterTag;
    private List<String> targetColumn;
    private List<Map<Object,Map>> analysisTargetColumns;

    public String getBussinessType() {
        return bussinessType;
    }

    public void setBussinessType(String bussinessType) {
        this.bussinessType = bussinessType;
    }

    public String getFileSplitTag() {
        return fileSplitTag;
    }

    public void setFileSplitTag(String fileSplitTag) {
        this.fileSplitTag = fileSplitTag;
    }

    public List<ColumnModel> getFileColumn() {
        return fileColumn;
    }

    public void setFileColumn(List<ColumnModel> fileColumn) {
        this.fileColumn = fileColumn;
    }

    public Map getFilterTag() {
        return filterTag;
    }

    public void setFilterTag(Map filterTag) {
        this.filterTag = filterTag;
    }

    public List<String> getTargetColumn() {
        return targetColumn;
    }

    public void setTargetColumn(List<String> targetColumn) {
        this.targetColumn = targetColumn;
    }

    public List<Map<Object, Map>> getAnalysisTargetColumns() {
        return analysisTargetColumns;
    }

    public void setAnalysisTargetColumns(List<Map<Object, Map>> analysisTargetColumns) {
        this.analysisTargetColumns = analysisTargetColumns;
    }
}
