package com.pandatv.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: likaiqing
 * Date: 2019-08-06
 * Description:
 */
public class SortByKeyTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Spark").getOrCreate();
        final JavaSparkContext ctx = JavaSparkContext.fromSparkContext(spark.sparkContext());

        List<String> data = Arrays.asList("a,110,a1", "b,122,b1", "c,123,c1", "a,210,a2", "b,212,b2", "a,310,a3", "b,312,b3", "a,410,a4", "b,412,b4");
        JavaRDD<String> javaRDD = ctx.parallelize(data);

        JavaPairRDD<String, Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String key) throws Exception {
                return new Tuple2<String, Integer>(key.split(",")[0], Integer.valueOf(key.split(",")[1]));
            }
        });

        final int topN = 3;
        JavaPairRDD<String, List<Integer>> combineByKeyRDD2 = javaPairRDD.combineByKey(new Function<Integer, List<Integer>>() {
            public List<Integer> call(Integer v1) throws Exception {
                List<Integer> items = new ArrayList<Integer>();
                items.add(v1);
                return items;
            }
        }, new Function2<List<Integer>, Integer, List<Integer>>() {
            public List<Integer> call(List<Integer> v1, Integer v2) throws Exception {
                if (v1.size() > topN) {
                    Integer item = Collections.min(v1);
                    v1.remove(item);
                    v1.add(v2);
                }
                return v1;
            }
        }, new Function2<List<Integer>, List<Integer>, List<Integer>>() {
            public List<Integer> call(List<Integer> v1, List<Integer> v2) throws Exception {
                v1.addAll(v2);
                while (v1.size() > topN) {
                    Integer item = Collections.min(v1);
                    v1.remove(item);
                }

                return v1;
            }
        });

        // 由K:String,V:List<Integer> 转化为 K:String,V:Integer
        // old:[(a,[210, 310, 410]), (b,[122, 212, 312]), (c,[123])]
        // new:[(a,210), (a,310), (a,410), (b,122), (b,212), (b,312), (c,123)]
        JavaRDD<Tuple2<String, Integer>> javaTupleRDD = combineByKeyRDD2.flatMap(new FlatMapFunction<Tuple2<String, List<Integer>>, Tuple2<String, Integer>>() {
            public Iterator<Tuple2<String, Integer>> call(Tuple2<String, List<Integer>> stringListTuple2) throws Exception {
                List<Tuple2<String, Integer>> items=new ArrayList<Tuple2<String, Integer>>();
                for(Integer v:stringListTuple2._2){
                    items.add(new Tuple2<String, Integer>(stringListTuple2._1,v));
                }
                return items.iterator();
            }
        });

        JavaRDD<Row> rowRDD = javaTupleRDD.map(new Function<Tuple2<String, Integer>, Row>() {
            public Row call(Tuple2<String, Integer> kv) throws Exception {
                String key = kv._1;
                Integer num = kv._2;

                return RowFactory.create(key, num);
            }
        });

        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("key", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("TopN_values", DataTypes.IntegerType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
        df.printSchema();
        df.show();

        spark.stop();
    }
}
