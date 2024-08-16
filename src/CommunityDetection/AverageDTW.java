package CommunityDetection;

import SparkClass.SplitPairELEWithMarkID;
import SparkClass.SplitPairELEWithMarkID_Float;
import com.Data.DataOperation;
import com.Data.ElectricProfile;
import com.Data.ElectricProfile_Float;
import com.FileOperate.HDFSOperate;
import com.Similarity.Distance;
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
import java.util.Iterator;
import java.util.Map;

/**
 * Created by hyy on 2017/11/9.
 */
public class AverageDTW {

    public static HashMap<Integer,ArrayList<Float>> getAverageSquence(JavaPairRDD<Integer,ElectricProfile_Float> dataSet, HashMap<Integer,ArrayList<Float>> averSquence,int win){

        //初始化
        if (averSquence.size()==0){
            class reduceForInitial implements Function2<ElectricProfile_Float,ElectricProfile_Float,ElectricProfile_Float>{
                @Override
                public ElectricProfile_Float call(ElectricProfile_Float electricProfile, ElectricProfile_Float electricProfile2) throws Exception {
                    double rate=Math.random();
                    if (rate<0.5){
                        return electricProfile;
                    }else {
                        return electricProfile2;
                    }
                }
            }

            class TrantoArray implements PairFunction<Tuple2<Integer,ElectricProfile_Float>,Integer,ArrayList<Float>>{
                @Override
                public Tuple2<Integer, ArrayList<Float>> call(Tuple2<Integer, ElectricProfile_Float> longElectricProfileTuple2) throws Exception {
                    return new Tuple2<Integer, ArrayList<Float>>(longElectricProfileTuple2._1,longElectricProfileTuple2._2().getPoints());
                }
            }

            averSquence.putAll(dataSet.reduceByKey(new reduceForInitial()).mapToPair(new TrantoArray()).collectAsMap());
        }

        final HashMap<Integer,ArrayList<Float>> _averSquence=averSquence;
        final int dtw_win=win;

        class getMulAlignment implements PairFunction<Tuple2<Integer,ElectricProfile_Float>,Integer,Tuple2<ArrayList<Float>,ArrayList<Integer>>>{
            @Override
            public Tuple2<Integer, Tuple2<ArrayList<Float>,ArrayList<Integer>>> call(Tuple2<Integer, ElectricProfile_Float> longArrayListTuple2) throws Exception {
                Distance dist=new Distance();
                ArrayList<ArrayList<Float>> _tmpValue=new ArrayList<ArrayList<Float>>();

                if (dtw_win>0){
                    _tmpValue=dist.getDTWMulAlignmentForFloat_win(_averSquence.get(longArrayListTuple2._1), longArrayListTuple2._2.getPoints(),dtw_win);
                }else {
                    _tmpValue=dist.getDTWMulAlignmentForFloat(_averSquence.get(longArrayListTuple2._1), longArrayListTuple2._2.getPoints());
                }


                ArrayList<Float> _value=new ArrayList<Float>();
                ArrayList<Integer> _size=new ArrayList<Integer>();

                for (int i = 0; i < _tmpValue.size(); i++) {
                    _value.add(DataOperation.getSumForflatArray(_tmpValue.get(i)));
                    _size.add(_tmpValue.get(i).size());
                }
                return new Tuple2<Integer, Tuple2<ArrayList<Float>, ArrayList<Integer>>>(longArrayListTuple2._1,new Tuple2<ArrayList<Float>, ArrayList<Integer>>(_value,_size));
            }
        }

        class reduceForSquence implements Function2<Tuple2<ArrayList<Float>, ArrayList<Integer>>,Tuple2<ArrayList<Float>, ArrayList<Integer>>,Tuple2<ArrayList<Float>, ArrayList<Integer>>>{
            @Override
            public Tuple2<ArrayList<Float>, ArrayList<Integer>> call(Tuple2<ArrayList<Float>, ArrayList<Integer>> arrayListArrayListTuple2, Tuple2<ArrayList<Float>, ArrayList<Integer>> arrayListArrayListTuple22) throws Exception {
                return new Tuple2<ArrayList<Float>, ArrayList<Integer>>(DataOperation.arrayPluForFloat(arrayListArrayListTuple2._1,arrayListArrayListTuple22._1),
                        DataOperation.arrayPluForInt(arrayListArrayListTuple2._2(),arrayListArrayListTuple22._2()));
            }
        }

        class getAverSquence implements PairFunction<Tuple2<Integer, Tuple2<ArrayList<Float>, ArrayList<Integer>>>,Integer,ArrayList<Float>>{
            @Override
            public Tuple2<Integer, ArrayList<Float>> call(Tuple2<Integer, Tuple2<ArrayList<Float>, ArrayList<Integer>>> arrayListArrayListTuple2) throws Exception {
                ArrayList<Float> retValue=new ArrayList<Float>();
                for (int i = 0; i < arrayListArrayListTuple2._2._1.size(); i++) {
                    retValue.add(arrayListArrayListTuple2._2()._1().get(i)/arrayListArrayListTuple2._2()._2().get(i));
                }
                return new Tuple2<Integer, ArrayList<Float>>(arrayListArrayListTuple2._1,retValue);
            }
        }

        HashMap<Integer,ArrayList<Float>> retValue=new HashMap<Integer, ArrayList<Float>>();
        retValue.putAll(dataSet.mapToPair(new getMulAlignment()).reduceByKey(new reduceForSquence()).mapToPair(new getAverSquence()).collectAsMap());
        return retValue;
    }

    public static void main(String[] args) throws IOException {

        String inputPath=args[0];
        int maxIter=Integer.parseInt(args[1]);
        String HDFSOutputPath=args[2];
        int dtw_win=Integer.parseInt(args[3]);


        SparkConf conf=new SparkConf().setAppName("AverageDTW");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);
        JavaPairRDD<Integer,ElectricProfile_Float> dataSet=lines.mapToPair(new SplitPairELEWithMarkID_Float());

        HashMap<Integer,ArrayList<Float>> averSquence=new HashMap<Integer, ArrayList<Float>>();
        for (int i = 0; i < maxIter; i++) {
            averSquence=getAverageSquence(dataSet,averSquence,dtw_win);
        }

        Iterator iter = averSquence.entrySet().iterator();
        String centersStr="";
        while (iter.hasNext()) {
            Map.Entry<Integer,ArrayList<Float>> entry = (Map.Entry<Integer,ArrayList<Float>>) iter.next();
            Integer key=entry.getKey();
            ArrayList<Float> val = entry.getValue();
            if (val!=null){
                centersStr+=key+",";
                centersStr+= DataOperation.ArrayToStrForFloat(val)+"\n";
            }
        }
        HDFSOperate.writeToHdfs(HDFSOutputPath+"centers/centers.txt", centersStr);
    }

}
