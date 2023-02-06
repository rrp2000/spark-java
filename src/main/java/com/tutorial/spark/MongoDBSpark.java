package com.tutorial.spark;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.tutorial.spark.model.PersonModel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.util.Arrays;
import java.util.List;

public class MongoDBSpark {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkMongoDBCsvExample")
                .master("local[*]")
                .config("spark.mongodb.input.uri", "mongodb://localhost/spring-mongodb.Book")
                .config("spark.mongodb.output.uri", "mongodb://localhost/spring-mongodb.Book")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    //reading the data
        Dataset<Row> df = MongoSpark.load(jsc).toDF();

//        df.select("name").limit(1).show();

//
//        //writing the data
//
//        WriteConfig writeConfig = WriteConfig.create(jsc);
//
//        // Document to write
//        Document document = new Document("_id", 5)
//                .append("name", "New from spark")
//                .append("description", "Very Good");
//
//        // Write Document to MongoDB
//        MongoSpark.save(jsc.parallelize(Arrays.asList(document)), writeConfig);


        //join operations
        List<PersonModel> people = Arrays.asList(
                new PersonModel("Rosan", 30,"a@gamil.com","hello"),
                new PersonModel("Brad", 25,"b@gamil.com","hello"),
                new PersonModel("Jane", 35,"c@gamil.com","hello")
                ,new PersonModel("Jay", 35,"d@gamil.com","hello")     );

// Convert the list to a Dataset
        Dataset<PersonModel> peopleDataset = spark.createDataset(people, Encoders.bean(PersonModel.class));

// Show the Dataset
//        peopleDataset.show();


//        Dataset<Row> result=df.join(peopleDataset, df.col("name").equalTo(peopleDataset.col("name")));
//         result.show();

//        left join
//        Dataset<Row> resultL=df.join(peopleDataset, df.col("name").equalTo(peopleDataset.col("name")), "left");
//         resultL.show();


//        left anti join
        Dataset<Row> resultA=peopleDataset.join(df, peopleDataset.col("name").equalTo(df.col("name")), "leftanti");
         resultA.show();
//

//        outer join
//        Dataset<Row> resultO=peopleDataset.join(df, peopleDataset.col("name").equalTo(df.col("name")), "outer");
//        resultO.show();


//         full join
//        Dataset<Row> resultF=peopleDataset.join(df, peopleDataset.col("name").equalTo(df.col("name")), "full");
//        resultF.show();

    }
}
