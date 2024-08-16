package CommunityDetection;

import SparkClass.SplitPairELEWithMarkID;
import com.Data.DataOperation;
import com.Data.ElectricProfile;
import com.Similarity.Distance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.*;

/**
 * Created by hyy on 2018/7/6.
 */
public class S_Dbw2v {


    //private ArrayList<Double> AverageDataset;
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

    //
    public static ArrayList<Double> getVar(ArrayList<ArrayList<Double>> data, ArrayList<Double> centers){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < centers.size(); i++) {
            double centerValue=centers.get(i);
            ArrayList<Double> dataValues=data.get(i);
            double dataValue=0;
            for (int j = 0; j < dataValues.size(); j++) {
                dataValue+=Math.pow(dataValues.get(j)-centerValue,2);
            }
            retValue.add(dataValue);
        }
        return retValue;
    }

    //按点计算方差
    public static List<Tuple2<Integer,ArrayList<Double>>> getVariance(JavaPairRDD<Integer,ElectricProfile> dataSet, HashMap<Integer,ArrayList<Double>> averSquence, final int win){
        final HashMap<Integer,ArrayList<Double>> _averSquence=averSquence;
        final int dtw_win=win;

        class getMulAlignment_2v implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<ArrayList<Double>,Integer>>{
            @Override
            public Tuple2<Integer, Tuple2<ArrayList<Double>,Integer>> call(Tuple2<Integer, ElectricProfile> longArrayListTuple2) throws Exception {
                Distance dist=new Distance();
                ArrayList<ArrayList<Double>> _tmpValue=new ArrayList<ArrayList<Double>>();

                _tmpValue=dist.getDTWMulAlignmentForDouble_win(_averSquence.get(longArrayListTuple2._1), longArrayListTuple2._2.getPoints(),dtw_win);

                ArrayList<Double> _value=getVar(_tmpValue,_averSquence.get(longArrayListTuple2._1));

                return new Tuple2<Integer, Tuple2<ArrayList<Double>, Integer>>(longArrayListTuple2._1,new Tuple2<ArrayList<Double>,Integer>(_value,1));
            }
        }

        class sumForVaiance implements Function2<Tuple2<ArrayList<Double>,Integer>,Tuple2<ArrayList<Double>,Integer>,Tuple2<ArrayList<Double>,Integer>>{
            @Override
            public Tuple2<ArrayList<Double>, Integer> call(Tuple2<ArrayList<Double>, Integer> doubleIntegerTuple2, Tuple2<ArrayList<Double>, Integer> doubleIntegerTuple22) throws Exception {
                return new Tuple2<ArrayList<Double>, Integer>(DataOperation.arrayPluForDouble(doubleIntegerTuple2._1(),doubleIntegerTuple22._1()),doubleIntegerTuple2._2+doubleIntegerTuple22._2);
            }
        }

        class getValue implements PairFunction<Tuple2<Integer,Tuple2<ArrayList<Double>,Integer>>,Integer,ArrayList<Double>>{
            @Override
            public Tuple2<Integer, ArrayList<Double>> call(Tuple2<Integer, Tuple2<ArrayList<Double>, Integer>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer, ArrayList<Double>>(integerTuple2Tuple2._1,DataOperation.ArrayMult(integerTuple2Tuple2._2._1,1.0/integerTuple2Tuple2._2._2));
            }
        }

        return dataSet.mapToPair(new getMulAlignment_2v()).reduceByKey(new sumForVaiance()).mapToPair(new getValue()).collect();
    }

    //计算S_Dbw的中间点的密度
    public static List<Tuple2<String,Long>> getDensityForMid(int win, JavaPairRDD<Integer,ElectricProfile> dataSet, List<Tuple2<String,ArrayList<Double>>> partMid, double nerThr, JavaSparkContext sc){
        final Broadcast<List<Tuple2<String,ArrayList<Double>>>> broPartDataSet=sc.broadcast(partMid);
        final double stdev=nerThr;
        final int dtw_win=win;
        class isNer implements PairFlatMapFunction<Tuple2<Integer,ElectricProfile>,String,Long> {
            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                List<Tuple2<String,ArrayList<Double>>> partDataTable=broPartDataSet.getValue();
                List<Tuple2<String,Long>> retValue=new ArrayList<Tuple2<String,Long>>();
                for (Tuple2<String,ArrayList<Double>> data:partDataTable
                        ) {
                    int index1=Integer.parseInt(data._1.split("-")[0]);
                    int index2=Integer.parseInt(data._1.split("-")[1]);
                    if ((integerElectricProfileTuple2._1.equals(index1))||(integerElectricProfileTuple2._1.equals(index2))){
                        Distance dist=new Distance();
                        double tmpDist=dist.getDTWDistanceForDouble(integerElectricProfileTuple2._2.getPoints(),data._2,dtw_win);
                        if (tmpDist<=stdev){
                            retValue.add(new Tuple2<String, Long>(data._1,1L));
                        }
                        //else {
                        //retValue.add(new Tuple2<String, Long>(data._1,0L));
                        //}
                    }
                }
                return retValue;
            }
        }

        class pulDen implements Function2<Long,Long,Long> {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        }

        return dataSet.flatMapToPair(new isNer()).reduceByKey(new pulDen()).collect();
    }

    //获取所有类的平均方差
    public static double getMeansVar(List<Tuple2<Integer,ArrayList<Double>>> var){

        double retValue=0;
        for (int i = 0; i < var.size(); i++) {
            retValue+=DataOperation.getArrayModel(var.get(i)._2);
        }
        retValue=Math.sqrt(retValue)/var.size();
        return retValue;
    }

    //获取聚类中心
    public static HashMap<Integer,ArrayList<Double>> getCenters(String inputPath) throws IOException {

        HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++){
            if(!fileList[i].isDirectory()){
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
                while((line = in.readLine()) != null){
                    line=line.replace("[","").replace("]","");
                    String[] tmpStr=line.split(",");
                    ArrayList<Double> s1=new ArrayList<Double>();
                    for (int j = 1; j < tmpStr.length; j++) {
                        s1.add(Double.parseDouble(tmpStr[j]));
                    }
                    retValue.put(Integer.parseInt(tmpStr[0]),s1);
                }
            }
        }
        in.close();
        fsi.close();
        return retValue;
    }

    //写文件
    public static void writeToHdfs(String pathStr,String context) throws IOException {
        Path path=new Path(pathStr);
        Configuration conf=new Configuration();
        FileSystem fs=null;
        fs=path.getFileSystem(conf);
        if (!fs.exists(path)){
            FSDataOutputStream outTmp=fs.create(path);
            outTmp.close();
        }
        FSDataOutputStream out=fs.append(path);
        out.write(context.getBytes("UTF-8"));
        out.flush();
        out.close();
        // fs.close();
    }

    //获取聚类中心
    public static HashMap<Integer,ArrayList<Double>> getCenters_2v(String path){

        //XYSeriesCollection sc=new XYSeriesCollection();
        HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                line=line.replace("[","").replace("]","");
                String[] tmpStr=line.split(",");
                ArrayList<Double> s1=new ArrayList<Double>();
                for (int i = 1; i < tmpStr.length; i++) {
                    s1.add(Double.parseDouble(tmpStr[i]));
                }
                retValue.put(Integer.parseInt(tmpStr[0]),s1);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return retValue;
    }

    //求两个中心的平均-动态时间扭曲情况下
    public static ArrayList<Double> getAverage(ArrayList<Double> x,ArrayList<Double> y,int win){

        ArrayList<Double> centers=new ArrayList<Double>();
        for (int i = 0; i < x.size(); i++) {
            centers.add(x.get(i));
        }
        Distance dist=new Distance();
        //ArrayList<ArrayList<Double>> _tmpValue=new ArrayList<ArrayList<Double>>();
        int repeat=0;
        while (repeat<100){
            ArrayList<ArrayList<Double>> _tmpValue1=dist.getDTWMulAlignmentForDouble_win(centers,x,win);
            ArrayList<ArrayList<Double>> _tmpValue2=dist.getDTWMulAlignmentForDouble_win(centers,y,win);
            ArrayList<Double> tmpCenters=new ArrayList<Double>();
            for (int i = 0; i < centers.size(); i++) {
                double v1=DataOperation.getSumFordoubleArray(_tmpValue1.get(i));
                double v2=DataOperation.getSumFordoubleArray(_tmpValue2.get(i));
                tmpCenters.add((v1+v2)/(_tmpValue1.get(i).size()+_tmpValue2.get(i).size()));
            }
            if (dist.getEuclideanDistance(tmpCenters,centers)==0){
                break;
            }
            centers=tmpCenters;
            repeat++;
        }

        System.out.print("########################### Repeat: "+repeat+"\n");
        return centers;
    }

    //获取S_Dbw指标，聚类效果越好指标值越小
    public static Tuple2<Double,Double> getS_Dbw(double dataSetVar,HashMap<Integer,ArrayList<Double>> averSquence,int dtw_win, JavaPairRDD<Integer,ElectricProfile> dataSet, JavaSparkContext sc ){
        dataSet.cache();
        DataOperation doper=new DataOperation();

        List<Tuple2<Integer,ArrayList<Double>>> centerVariance=getVariance(dataSet,averSquence,dtw_win);

        final double stdev=getMeansVar(centerVariance);
        final HashMap<Integer,ArrayList<Double>> _averSquence=averSquence;
        final int win=dtw_win;

        ArrayList<Tuple2<Integer,ArrayList<Double>>> centers=new ArrayList<Tuple2<Integer, ArrayList<Double>>>();

        for (Map.Entry<Integer, ArrayList<Double>> entry : averSquence.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<Double> value = entry.getValue();
            centers.add(new Tuple2<Integer, ArrayList<Double>>(key,value));
        }

        //region    dens_bw
        class isNer implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Long> {
            @Override
            public Tuple2<Integer, Long> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                double tmpDist=dist.getDTWDistanceForDouble(_averSquence.get(integerElectricProfileTuple2._1),integerElectricProfileTuple2._2.getPoints(),win);
                long ner=0;
                if (tmpDist<=stdev){
                    ner=1;
                }
                return new Tuple2<Integer,Long>(integerElectricProfileTuple2._1,ner);
            }
        }

        class pulDensity implements Function2<Long,Long,Long>{
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        }


        double dens_bw=0;
        HashMap<Integer,Long> centerDensity=new HashMap<Integer, Long>();
        Map<Integer,Long> tmpCenterDensity=dataSet.mapToPair(new isNer()).reduceByKey(new pulDensity()).collectAsMap();

        Iterator iter = tmpCenterDensity.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer,Long> entry = (Map.Entry<Integer,Long>) iter.next();
            Integer key=entry.getKey();
            Long value=entry.getValue();
            centerDensity.put(key,value);
        }

        List<Tuple2<String,ArrayList<Double>>> partMid=new ArrayList<Tuple2<String, ArrayList<Double>>>();
        for (int i = 0; i < centers.size(); i++) {
            for (int j = i+1; j <centers.size() ; j++) {
                partMid.add(new Tuple2<String, ArrayList<Double>>(
                        centers.get(i)._1+"-"+centers.get(j)._1,
                        getAverage(centers.get(i)._2(),centers.get(j)._2(),dtw_win)
                ));

                if (partMid.size()>=10000){
                    List<Tuple2<String,Long>> midDensity=getDensityForMid(win,dataSet,partMid,stdev,sc);
                    partMid.clear();
                    for (int k = 0; k < midDensity.size(); k++) {
                        long midDen=midDensity.get(k)._2;
                        String[] tmpKey=midDensity.get(k)._1.split("-");
                        int index1=Integer.parseInt(tmpKey[0]);
                        int index2=Integer.parseInt(tmpKey[1]);

                        long fDen=centerDensity.get(index1);
                        long sDen=centerDensity.get(index2);

                        long maxDen;
                        if (fDen>sDen){
                            maxDen=fDen;
                        }else {
                            maxDen=sDen;
                        }

                        if (maxDen<=0){
                            maxDen=1;
                        }
                        dens_bw+=midDen/maxDen;
                    }
                }
            }
        }

        if (partMid.size()>0){
            List<Tuple2<String,Long>> midDensity=getDensityForMid(win,dataSet,partMid,stdev,sc);
            partMid.clear();
            for (int k = 0; k < midDensity.size(); k++) {
                long midDen=midDensity.get(k)._2;

                String[] tmpKey=midDensity.get(k)._1.split("-");
                int index1=Integer.parseInt(tmpKey[0]);
                int index2=Integer.parseInt(tmpKey[1]);

                long fDen=centerDensity.get(index1);
                long sDen=centerDensity.get(index2);

                long maxDen;
                if (fDen>sDen){
                    maxDen=fDen;
                }else {
                    maxDen=sDen;
                }

                if (maxDen<=0){
                    maxDen=1;
                }
                dens_bw+=midDen/maxDen;
            }
        }
        dens_bw=2*dens_bw/(centers.size()*(centers.size()-1));
        //endregion

        double varCenters=0;
        for (int i = 0; i < centerVariance.size(); i++) {
            varCenters+=DataOperation.getArrayModel(centerVariance.get(i)._2);
        }

        //double dataSetVar=doper.getArrayModel(variance(dataSet.map(new trans())));

        double scat=varCenters/(dataSetVar*centerVariance.size());

        //return dens_bw+scat;
        return new Tuple2<Double,Double>(dens_bw,scat);
    }

    public static double getDataSetVar(JavaSparkContext sc,String dataSet_path,int win,String varSavestr) throws IOException {

        boolean runVar=false;
        double dataSetVaiance;
        Path path=new Path(varSavestr);
        Configuration conf=new Configuration();
        FileSystem fs=null;
        fs=path.getFileSystem(conf);
        if (!fs.exists(path)){
            runVar=true;
            FSDataOutputStream outTmp=fs.create(path);
            outTmp.close();
        }

        if (runVar){
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

            dataSetVaiance=DataOperation.getArrayModel(getVariance(dataSet_with_mark,dataSetCenter,win).get(0)._2);
            String tmpStr=dataSetVaiance+"\n";
            dataSet_with_mark.unpersist();
            FSDataOutputStream out=fs.append(path);
            out.write(tmpStr.getBytes("UTF-8"));
            out.flush();
            out.close();

        }else {
            FileSystem fs_2v = FileSystem.get(URI.create(varSavestr),new Configuration());
            BufferedReader in_2v = null;
            FSDataInputStream fsi_2v = null;
            String line = null;
            fsi_2v = fs_2v.open(new Path(varSavestr));
            in_2v = new BufferedReader(new InputStreamReader(fsi_2v,"UTF-8"));
            line = in_2v.readLine();
            dataSetVaiance=Double.parseDouble(line.trim());
            in_2v.close();
            fsi_2v.close();
        }

        return dataSetVaiance;
    }

    public static void main(String[] args) throws IOException {

        String dataSet_path=args[0];
        String dataSet_clustered_path=args[1];
        String centers_path=args[2];
        String save_path=args[3];
        int win =Integer.parseInt(args[4]);


        SparkConf conf=new SparkConf().setAppName("S_Dbw2v");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines_cluster = sc.textFile(dataSet_clustered_path);
        JavaPairRDD<Integer,ElectricProfile> dataSet_cluster=lines_cluster.mapToPair(new SplitPairELEWithMarkID());

        double dataSetVaiance=getDataSetVar(sc,dataSet_path,win,"hdfs://172.16.48.108:9000/Community/norBySum_filter/dataSetVar.txt");
        HashMap<Integer,ArrayList<Double>> centers=getCenters(centers_path);

        Tuple2<Double,Double> index=getS_Dbw(dataSetVaiance,centers,win,dataSet_cluster,sc);
        writeToHdfs(save_path,centers.size()+","+(index._1+index._2)+","+index._1+","+index._2+","+dataSetVaiance+","+(centers.size()*index._2*dataSetVaiance)+"\n");

    }

}
