package CommunityDetection;

import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;
import org.ujmp.core.Matrix;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hyy on 2018/6/28.
 */

//K中心聚类
public class PAM {
    private Matrix distanceMatrix;//=Matrix.Factory.zeros(a.size(),b.size());
    private int data_N;

    private ArrayList<String> dataSet=new ArrayList<String>();

    public PAM(String dataPath,int win){

        Distance dist=new Distance();
        ArrayList<ArrayList<Double>> data=getData(dataPath);
        System.out.println("Load the data, complete !");
        int len=data.size();
        data_N=len;
        distanceMatrix=Matrix.Factory.zeros(len,len);
        for (int i = 0; i <len ; i++) {
            for (int j = i+1; j < len; j++) {
                double tmpDis=dist.getDTWDistanceForDouble(data.get(i),data.get(j),win);
                distanceMatrix.setAsDouble(tmpDis,i,j);
                distanceMatrix.setAsDouble(tmpDis,j,i);
            }
        }
        System.out.println("Create the distance Matrix, complete !");
    }

    public void k_mediods(PAM pam, int k, int repeat,String savePath) throws InterruptedException {
         HashSet<Integer> clusterID=new HashSet<Integer>();
         int[] centers=new int[k];
         int[] data2center=new int[data_N];
         double[] data2center_dist=new double[data_N];
         double[] costs=new double[k];
         double[] replaceCost=new double[k];

         //region 初始化
        for (int i = 0; i < k; i++) {
            costs[i]=0;
            replaceCost[i]=0;
        }

         Random rand=new Random();
         while (clusterID.size()<k){
             clusterID.add(rand.nextInt(data_N));
         }
        System.out.println("Random select centers !");
        int index=0;
        for (Integer id: clusterID
             ) {
            centers[index]=id;
            index++;
        }

        for (int i = 0; i < data_N; i++) {
            int x=i;
            double[] d2c=getDistan2center(x,centers);
            int bestIndex=-1;
            double minDist=Double.MAX_VALUE;
            int bestCenter=-1;
            for (int j = 0; j <k ; j++) {
                if (d2c[j]<minDist){
                    bestIndex=centers[j];
                    minDist=d2c[j];
                    bestCenter=j;
                }
            }
            data2center[i]=bestIndex;
            data2center_dist[i]=minDist;
            costs[bestCenter]+=minDist;
        }
        System.out.println("Inital complete !");
        //endregion


        //region K-mediods(PAM) run
        boolean convergence=false;
        for (int repeatIndex = 0; repeatIndex < repeat; repeatIndex++) {
            convergence=true;
            for (int dataID = 0; dataID < data_N; dataID++) {
                if (!clusterID.contains(dataID)){

                    ExecutorService exe = Executors.newFixedThreadPool(k);
                    for (int centersIndex = 0; centersIndex <k ; centersIndex++) {
                        MyThread tmpThr=pam.new MyThread(repeatIndex+"-"+dataID+"-"+centersIndex,centers,data2center,data2center_dist,costs,replaceCost,dataID,centersIndex,distanceMatrix);
                        exe.execute(tmpThr);
                    }
                    exe.shutdown();
                    while (true) {
                        if (exe.isTerminated()) {
                            System.out.println("第 "+repeatIndex+"-"+dataID+" 替换结束了！");
                            break;
                        }
                        Thread.sleep(200);
                    }

                    //region 替换
                    int minCostIndex=getMin(replaceCost);
                    if (replaceCost[minCostIndex]<0){//发生替换，处理替换后的变换
                        convergence=false;
                        int repale=centers[minCostIndex];
                        costs[minCostIndex]=0;
                        centers[minCostIndex]=dataID;
                        for (int n = 0; n < data2center.length; n++) {
                            int tmpCenterId=data2center[n];
                            int bestCenterid=-1;
                            int bestCenterIndex=-1;
                            if (tmpCenterId==repale){//处理属于被替换的类的数据
                                double[] d2c=getDistan2center(n,centers);
                                double minDist=Double.MAX_VALUE;
                                for (int l= 0; l <centers.length ; l++) {
                                    if (d2c[l]<minDist){
                                        minDist=d2c[l];
                                        bestCenterid=centers[l];
                                        bestCenterIndex=l;
                                    }
                                }
                                //costs[minCostIndex]+=minDist;
                                costs[bestCenterIndex]+=minDist;
                                data2center[n]=bestCenterid;
                                data2center_dist[n]=minDist;

                            }else {
                                double tmpDist=distanceMatrix.getAsDouble(n,dataID);
                                if (tmpDist<data2center_dist[n]){
                                    costs[minCostIndex]+=tmpDist;
                                    data2center[n]=dataID;
                                    data2center_dist[n]=tmpDist;
                                }
                            }
                        }
                    }
                    //endregion

                    for (int i = 0; i < k; i++) {
                        replaceCost[i]=0;
                    }
                }
            }
            if (convergence){
                break;
            }
        }
        //endregion

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

    public int getMin(double[] data){
        int bestIndex=-1;
        double minData=Double.MAX_VALUE;
        for (int i = 0; i < data.length; i++) {
            if (data[i]<minData){
                minData=data[i];
                bestIndex=i;
            }
        }
        return bestIndex;
    }

    public double[] getDistan2center(int x, int[] y){
        double[] retValue=new double[y.length];
        for (int i = 0; i < y.length; i++) {
            retValue[i]=distanceMatrix.getAsDouble(x,y[i]);
        }
        return retValue;
    }

    public ArrayList<ArrayList<Double>> getData(String path){

        ArrayList<ArrayList<Double>> retValue=new ArrayList<ArrayList<Double>>();
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
                            retValue.add(tmpEle.getPoints());
                            dataSet.add(tmpStr[1]);
                            //int key=Integer.parseInt(tmpStr[0]);
                            //if (!retValue.containsKey(key)){
                                //retValue.put(key,new ArrayList<ArrayList<Double>>());
                            //}
                            //retValue.get(key).add(tmpEle.getPoints());
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

        PAM pam=new PAM(datapath,win);

        ArrayList<Integer> centersNum=pam.getCentersNum(centersNumPath);
        for (int i = 0; i < centersNum.size(); i++) {
            int k=centersNum.get(i);
            pam.k_mediods(pam,k,maxRepeat,savePath);
        }
    }
    class MyThread extends Thread{
        private int[] centers;
        private int[] data2center;
        private double[] data2center_dist;
        private double[] costs;
        private double[] replaceCost;
        private int ori;
        private int replace;
        private Matrix distanceMatrix_thr;

        public MyThread(String name, int[] centers,
                 int[] data2center,
                 double[] data2center_dist,
                 double[] costs,
                 double[] replaceCost,
                 int ori,
                 int replace, Matrix distanceMatrix_thr    ){
            super(name);
            this.centers=centers;
            this.data2center=data2center;
            this.data2center_dist=data2center_dist;
            this.costs=costs;
            this.replace=replace;
            this.replaceCost=replaceCost;
            this.ori=ori;
            this.distanceMatrix_thr=distanceMatrix_thr;

        }
        public double[] getDistan2center_thr(int x, int[] y){
            double[] retValue=new double[y.length];
            for (int i = 0; i < y.length; i++) {
                retValue[i]=distanceMatrix_thr.getAsDouble(x,y[i]);
            }
            return retValue;
        }

        public void run(){

            double tmpCost=-costs[replace];
            int[] center_thr=new int[centers.length];
            for (int i = 0; i < centers.length; i++) {
                if (i==replace){
                    center_thr[i]=ori;
                }else {
                    center_thr[i]=centers[i];
                }
            }

            for (int i = 0; i < data2center.length; i++) {
                int tmpCenterId=data2center[i];
                if (tmpCenterId==centers[replace]){//处理属于被替换的类的数据
                    double[] d2c=getDistan2center_thr(i,center_thr);
                    double minDist=Double.MAX_VALUE;
                    for (int j = 0; j <center_thr.length ; j++) {
                        if (d2c[j]<minDist){
                            minDist=d2c[j];
                        }
                    }
                    tmpCost+=minDist;
                }else {
                    double tmpDist=distanceMatrix_thr.getAsDouble(i,ori);
                    if (tmpDist<data2center_dist[ori]){
                        tmpCost-=data2center_dist[ori];
                        tmpCost+=tmpDist;
                    }
                }
            }
            synchronized (replaceCost){
                replaceCost[replace]=tmpCost;
            }
        }
    }
}
