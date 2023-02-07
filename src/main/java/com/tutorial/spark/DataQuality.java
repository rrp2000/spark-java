package com.tutorial.spark;

import com.tutorial.spark.model.PersonModel;
import com.tutorial.spark.model.QualityModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DataQuality {
    public static void main(String[] args) {

        try{
            SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();

            //data
            Dataset<Row> leftData = spark.createDataFrame(Arrays.asList(
                    new PersonModel("deben",21,"a@g.com/rerere",null),
                    new PersonModel("Rosan",25,"rrp@g.com","rerere"),
                    new PersonModel("Ranjan",75,"a@g.com","rerere"),
                    new PersonModel("patel",75,"a@g.com","rerere"),
                    new PersonModel("Rahul",25,"a@g.com","rerere"),
                    new PersonModel("Vishal",21,"a@g.com","rerere"),
                    new PersonModel("lalan",21,"a@g.com","rerere")
            ), PersonModel.class);
            Dataset<Row> rightData = spark.createDataFrame(Arrays.asList(
                    new PersonModel("deben",21,"a@g.com/rerere",null),
                    new PersonModel("Rosan",25,"a@g.com","rerere"),
                    new PersonModel("juju",21,"a@g.com","rerere"),
                    new PersonModel("Rabbit",25,"a@g.com","rerere"),
                    new PersonModel("sid",75,"a@g.com","rerere"),
                    new PersonModel("patel",75,"a@g.com","rerere"),
                    new PersonModel("Rahul",25,"a@g.com","rerere"),
                    new PersonModel("lalan",21,"a@g.com","rerere")
            ), PersonModel.class);


            Dataset<Row> analysis = spark.createDataFrame(Arrays.asList(), QualityModel.class);

            //data quality

            StructType schema = leftData.schema();

            for (StructField field : schema.fields()) {
                String columnName = field.name();

                List<?> columnValues = leftData.select(columnName).collectAsList();
                System.out.println(columnValues.get(0).toString().equals("[null]"));

                long totalRows  = columnValues.size();

                long completeCount = totalRows - columnValues.stream().filter(row->row.toString().equals("[null]")||row.toString().equals("[]")).count();

                long uniqueCount = leftData.dropDuplicates(columnName).count();


                analysis = analysis.union(spark.createDataFrame(Arrays.asList(new QualityModel(columnName, uniqueCount,completeCount,totalRows)), QualityModel.class));
            }

            analysis.show();

        }catch(Exception e){
            System.out.println(e.getMessage());
        }



    }
}
