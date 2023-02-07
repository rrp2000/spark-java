package com.tutorial.spark;

import com.tutorial.spark.model.PersonModel;
import org.apache.spark.sql.*;

import java.io.File;
import java.util.Arrays;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static scala.reflect.internal.util.NoPosition.show;


public class Main2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local").getOrCreate();

        //reading
        Dataset<Row> data = spark.createDataFrame(Arrays.asList(
                new PersonModel("Rosan",21,"a@g.com","rerere"),
                new PersonModel("Rosan",25,"a@g.com","rerere"),
                new PersonModel("Ranjan",75,"a@g.com","rerere"),
                new PersonModel("patel",75,"a@g.com","rerere"),
                new PersonModel("Rahul",25,"a@g.com","rerere"),
                new PersonModel("Vishal",21,"a@g.com","rerere"),
                new PersonModel("lalan",21,"a@g.com","rerere")
                ), PersonModel.class);
        data.show();

        //writing
//        data.write().option("header",true).mode(SaveMode.Append).csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate");


        //appending column

//        data = data.withColumn("pay",functions.abs(functions.rand()));
//        char[] letters = {'a','b','c','d','e','f','g','h','i'};
//        data = data.withColumn("pay",lit(letters[Math.random()]));

        data.show();

        //drop column
//
//        data = data.drop("pay");
//        data.show();

        //changing the data

//        data = data.withColumn("age",functions.when(data.col("age").gt(24),100).otherwise(data.col("age")));
//        data.show();
//        data.write().format("csv").option("header",true).mode(SaveMode.Overwrite).csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate");


        //distinct values

//        data = data.distinct();
//        data.show();

        //union

        //union
//        Dataset<Row> temp = spark.createDataFrame(Arrays.asList(
//                new PersonModel("patel",21,"a@g.com","rerere")
//        ), PersonModel.class);
//
//        data = data.union(temp);
//        data.show();

        //group by
//        data.groupBy("name").count().show();

        //de duplication

//        Dataset<Row> uniqueData = data.dropDuplicates("age");
//        Dataset<Row> removedData = data.except(uniqueData);

//        data.show();
//        uniqueData.show();
//        removedData.show();


        //join
//        Dataset<Row> leftData = spark.createDataFrame(Arrays.asList(
//                new PersonModel("Rosan",21,"a@g.com","rerere"),
//                new PersonModel("Rosan",25,"rrp@g.com","rerere"),
//                new PersonModel("Ranjan",75,"a@g.com","rerere"),
//                new PersonModel("patel",75,"a@g.com","rerere"),
//                new PersonModel("Rahul",25,"a@g.com","rerere"),
//                new PersonModel("Vishal",21,"a@g.com","rerere"),
//                new PersonModel("lalan",21,"a@g.com","rerere")
//        ), PersonModel.class);
//        Dataset<Row> rightData = spark.createDataFrame(Arrays.asList(
//                new PersonModel("deben",21,"a@g.com","rerere"),
//                new PersonModel("Rosan",25,"a@g.com","rerere"),
//                new PersonModel("juju",21,"a@g.com","rerere"),
//                new PersonModel("Rabbit",25,"a@g.com","rerere"),
//                new PersonModel("sid",75,"a@g.com","rerere"),
//                new PersonModel("patel",75,"a@g.com","rerere"),
//                new PersonModel("Rahul",25,"a@g.com","rerere"),
//                new PersonModel("lalan",21,"a@g.com","rerere")
//        ), PersonModel.class);

//        Dataset<Row> result = leftData.join(rightData, leftData.col("name").equalTo(rightData.col("name")),"inner");
//
//        result.show();

        //union

//        leftData.union(rightData).show();


        //split table using size

//        leftData = leftData.withColumn("rowNum", expr("row_number() over (order by 1)"));
//        for(int i = 0; i<leftData.count(); i+=2){
//            Dataset<Row> subData = leftData.filter(leftData.col("rowNum").$greater$eq(i + 1).and(leftData.col("rowNum").$less$eq(i + 2)));
//            subData = subData.drop("rowNum");
//            subData.write().csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate\\" + i + ".csv");
//        }


        //getting a single file

//        leftData = leftData.withColumn("rowNum", expr("row_number() over (order by 1)"));
//        for(int i = 0; i<leftData.count(); i+=2){
//            Dataset<Row> subData = leftData.filter(leftData.col("rowNum").$greater$eq(i + 1).and(leftData.col("rowNum").$less$eq(i + 2)));
//            subData = subData.drop("rowNum");
//            subData.write().csv("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate\\" + i);
//            File oldFile = new File("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate\\" + i);
//            File newFile = new File("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate\\" + i + ".csv");
//            oldFile.renameTo(newFile);
//        }
//        File folder = new File("C:\\Users\\ranja\\Downloads\\spark-starter\\src\\main\\java\\resources\\generate\\");
//        folder.delete();

    }
}
