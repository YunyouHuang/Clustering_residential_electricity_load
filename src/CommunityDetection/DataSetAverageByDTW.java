package CommunityDetection;

import SparkClass.SplitPairELEWithMarkID;
import com.Data.DataOperation;
import com.Data.ElectricProfile;
import com.Similarity.Distance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by hyy on 2018/7/15.
 */
public class DataSetAverageByDTW {

    //计算动态时间扭曲的均值
    public static HashMap<Integer,ArrayList<Double>> getAverageSquence(JavaPairRDD<Integer,ElectricProfile> dataSet, HashMap<Integer,ArrayList<Double>> averSquence, int win){

        //初始化
        if (averSquence.size()==0){
            class reduceForInitial implements Function2<ElectricProfile,ElectricProfile,ElectricProfile> {
                @Override
                public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                    double rate=Math.random();
                    if (rate<0.5){
                        return electricProfile;
                    }else {
                        return electricProfile2;
                    }
                }
            }

            class TrantoArray implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,ArrayList<Double>> {
                @Override
                public Tuple2<Integer, ArrayList<Double>> call(Tuple2<Integer, ElectricProfile> longElectricProfileTuple2) throws Exception {
                    return new Tuple2<Integer, ArrayList<Double>>(longElectricProfileTuple2._1,longElectricProfileTuple2._2().getPoints());
                }
            }

            averSquence.putAll(dataSet.reduceByKey(new reduceForInitial()).mapToPair(new TrantoArray()).collectAsMap());
        }

        final HashMap<Integer,ArrayList<Double>> _averSquence=averSquence;
        final int dtw_win=win;

        class getMulAlignment implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<ArrayList<Double>,ArrayList<Integer>>>{
            @Override
            public Tuple2<Integer, Tuple2<ArrayList<Double>,ArrayList<Integer>>> call(Tuple2<Integer, ElectricProfile> longArrayListTuple2) throws Exception {
                Distance dist=new Distance();
                ArrayList<ArrayList<Double>> _tmpValue=new ArrayList<ArrayList<Double>>();

                _tmpValue=dist.getDTWMulAlignmentForDouble_win(_averSquence.get(longArrayListTuple2._1), longArrayListTuple2._2.getPoints(),dtw_win);


                ArrayList<Double> _value=new ArrayList<Double>();
                ArrayList<Integer> _size=new ArrayList<Integer>();

                for (int i = 0; i < _tmpValue.size(); i++) {
                    _value.add(DataOperation.getSumFordoubleArray(_tmpValue.get(i)));
                    _size.add(_tmpValue.get(i).size());
                }
                return new Tuple2<Integer, Tuple2<ArrayList<Double>, ArrayList<Integer>>>(longArrayListTuple2._1,new Tuple2<ArrayList<Double>, ArrayList<Integer>>(_value,_size));
            }
        }

        class reduceForSquence implements Function2<Tuple2<ArrayList<Double>, ArrayList<Integer>>,Tuple2<ArrayList<Double>, ArrayList<Integer>>,Tuple2<ArrayList<Double>, ArrayList<Integer>>>{
            @Override
            public Tuple2<ArrayList<Double>, ArrayList<Integer>> call(Tuple2<ArrayList<Double>, ArrayList<Integer>> arrayListArrayListTuple2, Tuple2<ArrayList<Double>, ArrayList<Integer>> arrayListArrayListTuple22) throws Exception {
                return new Tuple2<ArrayList<Double>, ArrayList<Integer>>(DataOperation.arrayPluForDouble(arrayListArrayListTuple2._1,arrayListArrayListTuple22._1),
                        DataOperation.arrayPluForInt(arrayListArrayListTuple2._2(),arrayListArrayListTuple22._2()));
            }
        }

        class getAverSquence implements PairFunction<Tuple2<Integer, Tuple2<ArrayList<Double>, ArrayList<Integer>>>,Integer,ArrayList<Double>>{
            @Override
            public Tuple2<Integer, ArrayList<Double>> call(Tuple2<Integer, Tuple2<ArrayList<Double>, ArrayList<Integer>>> arrayListArrayListTuple2) throws Exception {
                ArrayList<Double> retValue=new ArrayList<Double>();
                for (int i = 0; i < arrayListArrayListTuple2._2._1.size(); i++) {
                    retValue.add(arrayListArrayListTuple2._2()._1().get(i)/arrayListArrayListTuple2._2()._2().get(i));
                }
                return new Tuple2<Integer, ArrayList<Double>>(arrayListArrayListTuple2._1,retValue);
            }
        }

        HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        retValue.putAll(dataSet.mapToPair(new getMulAlignment()).reduceByKey(new reduceForSquence()).mapToPair(new getAverSquence()).collectAsMap());
        return retValue;
    }


    public static void main(String[] args) throws IOException {
        String dataSet_path=args[0];
        //String dataSet_clustered_path=args[1];
        //String centers_path=args[2];
        String save_path=args[1];
        int win =Integer.parseInt(args[2]);

        SparkConf conf=new SparkConf().setAppName("DataSetAverageByDTW");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(dataSet_path);
        JavaPairRDD<Integer,ElectricProfile> dataSet=lines.mapToPair(new SplitPairELEWithMarkID());

        class tran2dataset implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return new Tuple2<Integer, ElectricProfile>(-1,integerElectricProfileTuple2._2);
            }
        }

        JavaPairRDD<Integer,ElectricProfile> dataSet_with_mark=dataSet.mapToPair(new tran2dataset());
        dataSet_with_mark.cache();

        HashMap<Integer, ArrayList<Double>> dataSetCenter=new HashMap<Integer, ArrayList<Double>>();
        for (int i = 0; i < 650; i++) {
            dataSetCenter=getAverageSquence(dataSet_with_mark,dataSetCenter,win);
        }


        Path path=new Path(save_path);
        Configuration conf_file=new Configuration();
        FileSystem fs=null;
        fs=path.getFileSystem(conf_file);
        if (!fs.exists(path)){
            FSDataOutputStream outTmp=fs.create(path);
            outTmp.close();
        }

        String tmpStr=dataSetCenter.get(-1)+"\n";
        dataSet_with_mark.unpersist();
        FSDataOutputStream out=fs.append(path);
        out.write(tmpStr.getBytes("UTF-8"));
        out.flush();
        out.close();
    }
}
