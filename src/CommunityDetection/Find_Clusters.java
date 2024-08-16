package CommunityDetection;

import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hyy on 2018/7/3.
 */
public class Find_Clusters {

    private double[][] distanceMatrix;//=Matrix.Factory.zeros(a.size(),b.size());
    private int data_N;
    //private double[] rowValue;
    private int[] v_order;
    private ArrayList<String> dataSet=new ArrayList<String>();

    public int[] initialCluster(int k){

        int[] centers=new int[k];
        for (int i = 0; i < k; i++) {
            centers[i]=v_order[i];
        }
        return centers;
    }

    public double getSum(double[] data){
        double retVaule=0;
        for (int i = 0; i < data.length; i++) {
            retVaule+=data[i];
        }
        return retVaule;
    }

    public static HashMap<Integer,ArrayList<Double>> getCenters(String path){

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

    public Find_Clusters(String dataPath,int win){

        String retValue="";
        Distance dist=new Distance();
        HashMap<Integer,ArrayList<ArrayList<Double>>> data=getData(dataPath);
        System.out.println("Load the data, complete !");

        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();

            if (value.size()<2){
                retValue+=key+","+value.get(0);
            }else {
                ArrayList<Double> value_cost=new ArrayList<Double>();
                for (int i = 0; i < value.size(); i++) {
                    value_cost.add(0.);
                }

                for (int i = 0; i < value.size(); i++) {
                    ArrayList<Double> tmpCenter=value.get(i);
                    for (int j = 0; j < value.size(); j++) {
                        double tmpDist=dist.getDTWDistanceForDouble(value.get(j),tmpCenter,win);

                    }
                }


                //center2cost.put(key,value_cost);
            }

        }


    }

    public int[] sortInsert(double[] data){
        int[] retVaule=new int[data.length];
        for (int i = 0; i < data.length; i++) {
            retVaule[i]=i;
        }
        for (int i = 1; i < data.length; i++) {
            double tmpData=data[i];
            int tmpIdex=i;
            int j;
            for (j = i; j >0&&(tmpData<data[j-1]) ; j--) {
                data[j]=data[j-1];
                retVaule[j]=retVaule[j-1];
            }
            data[j]=tmpData;
            retVaule[j]=tmpIdex;
        }
        return retVaule;
    }

    public int getMinIndex(ArrayList<Double> data){
        int bestIndex=-1;
        double bestValue=Double.MAX_VALUE;

        for (int i = 0; i < data.size(); i++) {
            if (data.get(i)<bestValue){
                bestValue=data.get(i);
                bestIndex=i;
            }
        }
        return bestIndex;
    }

    public void k_mediods( int k, int repeat,String savePath) throws InterruptedException {

        int[] centers;
        int[] data2center=new int[data_N];
        HashMap<Integer,ArrayList<Integer>> center2data=new HashMap<Integer, ArrayList<Integer>>();
        double costSum=0;
        double preCostSum=0;

        //region 初始化

        centers=initialCluster(k);
        System.out.println("Select centers !");


        for (int i = 0; i < data_N; i++) {
            int x=i;
            double[] d2c=getDistan2center(x,centers);
            int bestIndex=-1;
            double minDist=Double.MAX_VALUE;
            for (int j = 0; j <k ; j++) {
                if (d2c[j]<minDist){
                    bestIndex=centers[j];
                    minDist=d2c[j];
                }
            }
            data2center[i]=bestIndex;
            if (!center2data.containsKey(bestIndex)){
                center2data.put(bestIndex,new ArrayList<Integer>());
            }
            center2data.get(bestIndex).add(i);
            //data2center_dist[i]=minDist;
            costSum+=minDist;
        }
        System.out.println("Inital complete !");
        //endregion

        int repeat_index=0;
        while ((repeat_index<repeat)&&(costSum!=preCostSum)){
            System.out.println("Cluster number: "+k+"   Repeat index "+repeat_index);

            HashMap<Integer,ArrayList<Double>> center2cost=new HashMap<Integer, ArrayList<Double>>();

            //region 初始化代价
            for (Map.Entry<Integer, ArrayList<Integer>> entry : center2data.entrySet()) {
                Integer key = entry.getKey();
                ArrayList<Integer> value = entry.getValue();
                ArrayList<Double> value_cost=new ArrayList<Double>();
                for (int i = 0; i < value.size(); i++) {
                    value_cost.add(0.);
                }
                center2cost.put(key,value_cost);
            }
            //endregion

            //region 计算代价
            for (int i = 0; i < data_N; i++) {
                int key=data2center[i];
                ArrayList<Integer> clusteringID=center2data.get(key);
                for (int j = 0; j < clusteringID.size(); j++) {
                    double tmpDist=distanceMatrix[clusteringID.get(j)][i];
                    center2cost.get(key).set(j,center2cost.get(key).get(j)+tmpDist);
                }

            }
            //endregion

            //region 更新聚类中心
            int[] tmpCenters=new int[centers.length];
            int index=0;
            for (Map.Entry<Integer, ArrayList<Double>> entry : center2cost.entrySet()) {
                Integer key = entry.getKey();
                ArrayList<Double> value = entry.getValue();
                //ArrayList<Double> value_cost=new ArrayList<Double>();
                int tmpIndex=getMinIndex(value);
                tmpCenters[index]=center2data.get(key).get(tmpIndex);
                index++;
            }
            //endregion

            centers=tmpCenters;
            preCostSum=costSum;
            costSum=0;
            center2data=new HashMap<Integer, ArrayList<Integer>>();

            //region 计算代价，重新分配类别
            for (int i = 0; i < data_N; i++) {
                int x=i;
                double[] d2c=getDistan2center(x,centers);
                int bestIndex=-1;
                double minDist=Double.MAX_VALUE;
                for (int j = 0; j <k ; j++) {
                    if (d2c[j]<minDist){
                        bestIndex=centers[j];
                        minDist=d2c[j];

                    }
                }
                data2center[i]=bestIndex;
                if (!center2data.containsKey(bestIndex)){
                    center2data.put(bestIndex,new ArrayList<Integer>());
                }
                center2data.get(bestIndex).add(i);
                //data2center_dist[i]=minDist;
                costSum+=minDist;
            }
            System.out.println("Inital complete !");
            //endregion

            repeat_index++;

        }


        String dataStr="";
        for (int i = 0; i < data_N; i++) {
            dataStr+="("+data2center[i]+","+dataSet.get(i)+")\n";
        }
        FileWrite.WriteTxt(dataStr,savePath+"kmediods_"+k+"/data/data.txt");
        String centerStr="";
        for (int i = 0; i < k; i++) {
            centerStr+=centers[i]+","+new ElectricProfile(dataSet.get(centers[i])).getPoints()+"\n";
        }
        FileWrite.WriteTxt(centerStr,savePath+"kmediods_"+k+"/centers/centers.txt");

    }

    public ArrayList<Integer> getCentersNum(String path){

        ArrayList<Integer> retValue=new ArrayList<Integer>();
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmpStr=line.split(",");
                retValue.add(Integer.parseInt(tmpStr[0]));
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

    public double[] getDistan2center(int x, int[] y){
        double[] retValue=new double[y.length];
        for (int i = 0; i < y.length; i++) {
            retValue[i]=distanceMatrix[x][y[i]];
        }
        return retValue;
    }

    public HashMap<Integer,ArrayList<ArrayList<Double>>> getData(String path){

        HashMap<Integer,ArrayList<ArrayList<Double>>> retValue=new HashMap<Integer, ArrayList<ArrayList<Double>>>();
        File file = new File(path);
        if (file.isDirectory()) {
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readfile = new File(path + "/" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(new FileInputStream(readfile), "UTF-8"));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            String[] tmpStr=line.substring(1,line.length()-1).split(",",2);
                            ElectricProfile tmpEle=new ElectricProfile(tmpStr[1]);
                            //retValue.add(tmpEle.getPoints());
                            //dataSet.add(tmpStr[1]);
                            int key=Integer.parseInt(tmpStr[0]);
                            if (!retValue.containsKey(key)){
                            retValue.put(key,new ArrayList<ArrayList<Double>>());
                            }
                            retValue.get(key).add(tmpEle.getPoints());
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return retValue;
    }

    public static void main(String[] args) throws InterruptedException {

        String datapath=args[0];
        int win=Integer.parseInt(args[1]);
        String centersNumPath=args[2];
        int maxRepeat=Integer.parseInt(args[3]);
        String savePath=args[4];

        K_Mediod_Faster KM_faster=new K_Mediod_Faster(datapath,win);

        ArrayList<Integer> centersNum=KM_faster.getCentersNum(centersNumPath);
        for (int i = 0; i < centersNum.size(); i++) {
            int k=centersNum.get(i);
            KM_faster.k_mediods(k,maxRepeat,savePath,i);
        }
    }

}
