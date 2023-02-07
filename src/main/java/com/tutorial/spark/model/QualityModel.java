package com.tutorial.spark.model;

import org.apache.spark.sql.types.DataType;

public class QualityModel {
    private String column_name;
    private long distinctness;
    private long completeness;

    private long totalNoOfRows;

    public long getTotalNoOfRows() {
        return totalNoOfRows;
    }

    public void setTotalNoOfRows(long totalNoOfRows) {
        this.totalNoOfRows = totalNoOfRows;
    }

    public QualityModel(String column_name, long distinctness, long completeness, long totalNoOfRows) {
        this.column_name = column_name;
        this.distinctness = distinctness;
        this.completeness = completeness;
        this.totalNoOfRows = totalNoOfRows;
    }

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }

    public long getDistinctness() {
        return distinctness;
    }

    public void setDistinctness(long distinctness) {
        this.distinctness = distinctness;
    }

    public long getCompleteness() {
        return completeness;
    }

    public void setCompleteness(long completeness) {
        this.completeness = completeness;
    }



}
