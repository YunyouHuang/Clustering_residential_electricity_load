package CommunityDetection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by hdp on 17-8-21.
 */
public class GetCluster {


    public static void main(String[] args) throws IOException {
        String inputData=args[0];
        String inputGraphCommnect=args[1];
        String savePath=args[2];

        SparkConf conf=new SparkConf().setAppName("GetClusters");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> linesData = sc.textFile(inputData);
        JavaRDD<String> linesGraph=sc.textFile(inputGraphCommnect);

        class splitData implements PairFunction<String,Long,String>{
            @Override
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                return new Tuple2<Long, String>(Long.parseLong(tmpStr[0]),tmpStr[1]);
            }
        }

        class splitGraph implements PairFunction<String,Long,Long>{
            @Override
            public Tuple2<Long, Long> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                return new Tuple2<Long, Long>(Long.parseLong(tmpStr[0]),Long.parseLong(tmpStr[1]));
            }
        }

        class getDataWithClusterID implements PairFunction<Tuple2<Long,Tuple2<Long,String>>,Long,String>{
            @Override
            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> longTuple2Tuple2) throws Exception {
                return longTuple2Tuple2._2;
            }
        }

        linesGraph.mapToPair(new splitGraph()).join(linesData.mapToPair(new splitData())).mapToPair(new getDataWithClusterID()).saveAsTextFile(savePath);
    }
}
