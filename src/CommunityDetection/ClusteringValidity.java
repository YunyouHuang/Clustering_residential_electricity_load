package CommunityDetection;

import com.Data.DataOperation;
import com.Data.ElectricProfile;
import com.Similarity.Distance;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import com.Data.*;

import java.util.*;

/**
 * Created by hyy on 2017/12/7.
 */
public class ClusteringValidity {

    //获取DBI指标，聚类效果越好指标值越小
    public static double getDBI(JavaPairRDD<Integer,ElectricProfile> dataSet, HashMap<Integer,ElectricProfile> centers ){

        final HashMap<Integer,ElectricProfile> center=centers;

        class claculate implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<Double,Long>> {
            @Override
            public Tuple2<Integer, Tuple2<Double, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                return new Tuple2<Integer, Tuple2<Double, Long>>(
                        integerElectricProfileTuple2._1,
                        new Tuple2<Double, Long>(
                                dist.getPowDistance(integerElectricProfileTuple2._2.getPoints(),center.get(integerElectricProfileTuple2._1).getPoints()),1L
                        )
                );
            }
        }

        class pluEle implements Function2<Tuple2<Double,Long>,Tuple2<Double,Long>,Tuple2<Double,Long>> {
            @Override
            public Tuple2<Double, Long> call(Tuple2<Double, Long> doubleLongTuple2, Tuple2<Double, Long> doubleLongTuple22) throws Exception {
                return new Tuple2<Double, Long>(
                        doubleLongTuple2._1+doubleLongTuple22._1,doubleLongTuple2._2+doubleLongTuple22._2
                );
            }
        }

        class calSI implements PairFunction<Tuple2<Integer,Tuple2<Double,Long>>,Integer,Double>{
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer,Double>(
                        integerTuple2Tuple2._1,
                        Math.sqrt(integerTuple2Tuple2._2._1/integerTuple2Tuple2._2._2)
                );
            }
        }

        List<Tuple2<Integer,Double>> SI=dataSet.mapToPair(new claculate()).reduceByKey(new pluEle()).mapToPair(new calSI()).collect();
        double BDI=0;
        for (int i = 0; i < SI.size(); i++) {
            Tuple2<Integer,Double> tmpSI=SI.get(i);
            double maxR=0;
            for (int j = 0; j < SI.size(); j++) {
                if (i!=j){
                    Tuple2<Integer,Double> tmpSI2=SI.get(j);
                    Distance dist=new Distance();
                    double tmpDistCenters=dist.getEuclideanDistance(center.get(tmpSI._1).getPoints(),center.get(tmpSI2._1).getPoints());
                    //System.out.print(tmpSI._1+"-"+tmpSI2._1+":    "+tmpDistCenters+"\n");
                    double tmpR=(tmpSI._2+tmpSI2._2)/tmpDistCenters;
                    //System.out.println();
                    if (tmpR>maxR){
                        maxR=tmpR;
                    }
                }
            }
            //double tmpKK=tmpSI._2;
            //System.out.print("index-"+i+":  "+tmpKK+"\n");
            BDI+=maxR;
        }
        return BDI/center.size();
    }


    //获取DBI指标，聚类效果越好指标值越小
    public static double getDBIForDTW(JavaPairRDD<Integer,ElectricProfile> dataSet, HashMap<Integer,ElectricProfile> centers ){

        final HashMap<Integer,ElectricProfile> center=centers;

        class claculate implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<Double,Long>> {
            @Override
            public Tuple2<Integer, Tuple2<Double, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                return new Tuple2<Integer, Tuple2<Double, Long>>(
                        integerElectricProfileTuple2._1,
                        new Tuple2<Double, Long>(
                                Math.pow(dist.getDTWDistance(integerElectricProfileTuple2._2.getPoints(),center.get(integerElectricProfileTuple2._1).getPoints()),2),1L
                        )
                );
            }
        }

        class pluEle implements Function2<Tuple2<Double,Long>,Tuple2<Double,Long>,Tuple2<Double,Long>> {
            @Override
            public Tuple2<Double, Long> call(Tuple2<Double, Long> doubleLongTuple2, Tuple2<Double, Long> doubleLongTuple22) throws Exception {
                return new Tuple2<Double, Long>(
                        doubleLongTuple2._1+doubleLongTuple22._1,doubleLongTuple2._2+doubleLongTuple22._2
                );
            }
        }

        class calSI implements PairFunction<Tuple2<Integer,Tuple2<Double,Long>>,Integer,Double>{
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer,Double>(
                        integerTuple2Tuple2._1,
                        Math.sqrt(integerTuple2Tuple2._2._1/integerTuple2Tuple2._2._2)
                );
            }
        }

        List<Tuple2<Integer,Double>> SI=dataSet.mapToPair(new claculate()).reduceByKey(new pluEle()).mapToPair(new calSI()).collect();
        double BDI=0;
        for (int i = 0; i < SI.size(); i++) {
            Tuple2<Integer,Double> tmpSI=SI.get(i);
            double maxR=0;
            for (int j = 0; j < SI.size(); j++) {
                if (i!=j){
                    Tuple2<Integer,Double> tmpSI2=SI.get(j);
                    Distance dist=new Distance();
                    double tmpDistCenters=dist.getDTWDistance(center.get(tmpSI._1).getPoints(),center.get(tmpSI2._1).getPoints());
                    //System.out.print(tmpSI._1+"-"+tmpSI2._1+":    "+tmpDistCenters+"\n");
                    double tmpR=(tmpSI._2+tmpSI2._2)/tmpDistCenters;
                    //System.out.println();
                    if (tmpR>maxR){
                        maxR=tmpR;
                    }
                }
            }
            //double tmpKK=tmpSI._2;
            //System.out.print("index-"+i+":  "+tmpKK+"\n");
            BDI+=maxR;
        }
        return BDI/center.size();
    }

    //计算S_Dbw的中间点的密度
    public static List<Tuple2<String,Long>> getDensityForMid(JavaPairRDD<Integer,ElectricProfile> dataSet,
                                                             List<Tuple2<String,ArrayList<Double>>> partMid,
                                                             double nerThr,JavaSparkContext sc){
        final Broadcast<List<Tuple2<String,ArrayList<Double>>>> broPartDataSet=sc.broadcast(partMid);
        final double stdev=nerThr;
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
                        double tmpDist=dist.getEuclideanDistance(integerElectricProfileTuple2._2.getPoints(),data._2);
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

        class pulDen implements Function2<Long,Long,Long>{
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        }

        return dataSet.flatMapToPair(new isNer()).reduceByKey(new pulDen()).collect();
    }


    //获取S_Dbw指标，聚类效果越好指标值越小
    public static double getS_Dbw(JavaPairRDD<Integer,ElectricProfile> dataSet,
                                  HashMap<Integer,ElectricProfile> centers, JavaSparkContext sc ){
        dataSet.cache();
        DataOperation doper=new DataOperation();
        final HashMap<Integer,ElectricProfile> fineCenters=centers;
        class trans implements Function<Tuple2<Integer,ElectricProfile>,ElectricProfile> {
            @Override
            public ElectricProfile call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return integerElectricProfileTuple2._2;
            }
        }

        Map<Integer,ArrayList<Double>> clusterMeans=DigitalFeature.getMeansForCluster(dataSet);
        List<Tuple2<Integer,ArrayList<Double>>> clusterVar=DigitalFeature.getVarianceForCluster(dataSet,clusterMeans);
        final double stdev=DigitalFeature.getMeansVar(clusterVar);

        //region    dens_bw
        class isNer implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Long>{
            @Override
            public Tuple2<Integer, Long> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                double tmpDist=dist.getEuclideanDistance(integerElectricProfileTuple2._2.getPoints(),fineCenters.get(integerElectricProfileTuple2._1).getPoints());
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
        for (int i = 0; i < centers.size(); i++) {
            if (!centerDensity.containsKey(i)){
                centerDensity.put(i,0L);
            }
        }
        List<Tuple2<String,ArrayList<Double>>> partMid=new ArrayList<Tuple2<String, ArrayList<Double>>>();
        for (int i = 0; i < centers.size(); i++) {
            for (int j = i+1; j <centers.size() ; j++) {
                partMid.add(new Tuple2<String, ArrayList<Double>>(
                        i+"-"+j,
                        doper.ArrayDivid(doper.arrayPlu(centers.get(i).getPoints(),centers.get(j).getPoints()),2)
                ));
                if (partMid.size()>=10000){
                    List<Tuple2<String,Long>> midDensity=getDensityForMid(dataSet,partMid,stdev,sc);
                    partMid.clear();
                    for (int k = 0; k < midDensity.size(); k++) {
                        long midDen=midDensity.get(k)._2;
                        int index1=Integer.parseInt(midDensity.get(k)._1.split("-")[0]);
                        int index2=Integer.parseInt(midDensity.get(k)._1.split("-")[1]);

                        long fDen=centerDensity.get(index1);
                        long sDen=centerDensity.get(index2);

                        long maxDen=1;
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
            List<Tuple2<String,Long>> midDensity=getDensityForMid(dataSet,partMid,stdev,sc);
            partMid.clear();
            for (int k = 0; k < midDensity.size(); k++) {
                long midDen=midDensity.get(k)._2;
                int index1=Integer.parseInt(midDensity.get(k)._1.split("-")[0]);
                int index2=Integer.parseInt(midDensity.get(k)._1.split("-")[1]);

                long fDen=centerDensity.get(index1);
                long sDen=centerDensity.get(index2);

                long maxDen=1;
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
        for (int i = 0; i < clusterVar.size(); i++) {
            varCenters+=doper.getArrayModel(clusterVar.get(i)._2);
        }

        double dataSetVar=doper.getArrayModel(DigitalFeature.variance(dataSet.map(new trans())));

        double scat=varCenters/(centers.size()*dataSetVar);

        return dens_bw+scat;
    }


    //region
    /*
    //WC指标，聚类效果越好指标值越小
    public static double getWC(JavaPairRDD<Integer,ElectricProfile> dataSet, HashMap<Integer,ElectricProfile> centers, int calFlag){
        final HashMap<Integer,ElectricProfile> _center=centers;
        final int flag=calFlag;

        class
    }
    */
    //enregion
}
