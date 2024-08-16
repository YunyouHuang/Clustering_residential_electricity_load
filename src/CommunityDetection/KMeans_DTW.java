package CommunityDetection;

import clusterMethod.Element.Cluster;
import com.Data.DataOperation;
import com.Data.ElectricProfile;
import com.Data.ElectricProfile_Float;
import com.Similarity.Distance;
import com.Transform.FftConv;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * Created by hyy on 2017/12/25.
 */
public class KMeans_DTW {

    public static HashMap<Integer,ArrayList<Float>> kMeansClustering(int clustersNum, double convergeDist, int maxCount, JavaRDD<ElectricProfile_Float> dataSet,
                                                                          JavaSparkContext sc, int calDistFlag, HashMap<Integer,ArrayList<Float>> intiCenters){

        HashMap<Integer,ArrayList<Float>> _clusterCenters=new HashMap<Integer,ArrayList<Float>>();
        final int distFlag=calDistFlag;
        float tmpDist = Float.MAX_VALUE;
        JavaRDD<ElectricProfile_Float> profileSet=dataSet;

        //region 初始化聚类中心
        if (intiCenters.size()>0){
            Iterator iter = intiCenters.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Integer,ArrayList<Float>> entry = (Map.Entry<Integer,ArrayList<Float>>) iter.next();
                ArrayList<Float> val = entry.getValue();
                Integer key=entry.getKey();
                _clusterCenters.put(key,val);
            }
            clustersNum=intiCenters.size();
        }else {
            List<ElectricProfile_Float> centers=profileSet.takeSample(false,clustersNum,30);
            int index=0;
            for (ElectricProfile_Float profile:centers) {
                _clusterCenters.put(index,profile.getPoints());
                index++;
            }
        }
        //endregion

        final int clusterNum=clustersNum;
        int count=0;
        while (tmpDist>convergeDist){
            tmpDist=0;
            HashMap<Integer,ArrayList<Float>> tmpCenters=new HashMap<Integer, ArrayList<Float>>();
            tmpCenters.putAll(_clusterCenters);
            final Broadcast<HashMap<Integer,ArrayList<Float>>> broClusterCenters=sc.broadcast(tmpCenters);

            class tupleClusters implements PairFunction<ElectricProfile_Float,Integer,ElectricProfile_Float>{
                @Override
                public Tuple2<Integer, ElectricProfile_Float> call(ElectricProfile_Float electricProfile) throws Exception {
                    HashMap<Integer,ArrayList<Float>> clusterCenters=broClusterCenters.getValue();
                    double minDis=Double.MAX_VALUE;
                    int bestIndex=-1;
                    for (int i = 0; i < clusterNum; i++) {
                        double tmpdist;
                        if (distFlag==0){
                            Distance dist=new Distance();
                            tmpdist=dist.getDTWDistanceForFloat(clusterCenters.get(i),electricProfile.getPoints());
                        }else if(distFlag==1){
                            Tuple2<Integer,Double> tmp= FftConv.getMaxNNCc(DataOperation.Float2Doubel(clusterCenters.get(i)),DataOperation.Float2Doubel(electricProfile.getPoints()));
                            tmpdist=1-tmp._2;
                        }else {
                            Distance dist=new Distance();
                            tmpdist= dist.getEuclideanDistance(DataOperation.Float2Doubel(clusterCenters.get(i)),DataOperation.Float2Doubel(electricProfile.getPoints()));
                        }
                        //Distance dis=new Distance();
                        //=dis.getDTWDistance(clusterCenters.get(i).getPoints(),electricProfile.getPoints());
                        if (tmpdist<minDis){
                            minDis=tmpdist;
                            bestIndex=i;
                        }
                    }
                    return new Tuple2<Integer, ElectricProfile_Float>(bestIndex,electricProfile);
                }
            }

            JavaPairRDD<Integer,ElectricProfile_Float> clusters=profileSet.mapToPair(new tupleClusters());
            HashMap<Integer,ArrayList<Float>> newCenters=new HashMap<Integer, ArrayList<Float>>();
            newCenters=AverageDTW.getAverageSquence(clusters,tmpCenters,96);
            if (newCenters.size()==_clusterCenters.size()){
                Iterator iter = newCenters.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<Integer,ArrayList<Float>> entry = (Map.Entry<Integer,ArrayList<Float>>) iter.next();
                    Integer key=entry.getKey();
                    ArrayList<Float> val = entry.getValue();
                    if (!_clusterCenters.containsKey(key)){
                        tmpDist=Float.MAX_VALUE;
                        break;
                    }else {
                        ArrayList<Float> val2=new ArrayList<Float>();
                        Distance dist=new Distance();
                        tmpDist+=dist.getDTWDistanceForFloat(val,val2);
                    }
                }
            }
            count++;
            if (count>maxCount)
                break;
            broClusterCenters.unpersist();
        }
        return _clusterCenters;
    }
    public static void main(String[] args) throws IOException {
        String inputPath=args[0];
        String HDFSOutputPath=args[1];
        int clustersNum=Integer.parseInt(args[2]);
        double convergeDist=Double.parseDouble(args[3]);
        int maxCount=Integer.parseInt(args[4]);
        int calDistFlag=Integer.parseInt(args[5]);
        int minNum=Integer.parseInt(args[6]);
        int adFlag=Integer.parseInt(args[7]);
        double vioThr=Double.parseDouble(args[8]);
        
        SparkConf conf=new SparkConf().setAppName("Kmeans");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);
    }
}
