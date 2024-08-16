package CommunityDetection;

import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hyy on 2019/4/12.
 */
public class SF_EU {

    private ArrayList<Double> dataSetCenter=new ArrayList<Double>();
    private boolean isInitial=false;

    public static HashMap<Integer,ArrayList<ArrayList<Double>>> getData(String path){

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

    public static HashMap<Integer,ArrayList<Double>> clearCenters(HashMap<Integer,ArrayList<ArrayList<Double>>> data,HashMap<Integer,ArrayList<Double>> centers){
        HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        for (Map.Entry<Integer, ArrayList<Double>> entry : centers.entrySet()) {
            int key=entry.getKey();
            if (data.containsKey(key)){
                retValue.put(key,entry.getValue());
            }
        }
        return retValue;
    }

    public String evaluation(String dataPath,String centerPath){
        HashMap<Integer,ArrayList<ArrayList<Double>>> data=getData(dataPath);
        //HashMap<Integer,ArrayList<Double>> centers=getCenters(centerPath);
        HashMap<Integer,ArrayList<Double>> centers=clearCenters(data,getCenters(centerPath));
        if (!isInitial){
            isInitial=true;
            dataSetCenter=getDatacenters(data);
        }
        //ArrayList<Double> dataSetCenter=getDatacenters(data);


        Distance dist=new Distance();
        int rare=0;
        double fen=0;
        int dataNum=0;
        double bcd =0;
        double wcd=0;

        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();
            dataNum+=value.size();

            double c_d_Distance=dist.getEuclideanDistance(centers.get(key),dataSetCenter);
            c_d_Distance=c_d_Distance*value.size();
            bcd+=c_d_Distance;

            double tmpSum=0;
            if (value.size()>1){
                ArrayList<Double> center=centers.get(key);
                for (int i = 0; i < value.size(); i++) {
                    ArrayList<Double> tmpData=value.get(i);
                    tmpSum+=dist.getEuclideanDistance(center,tmpData);
                }
            }else {
                rare++;
            }
            tmpSum=tmpSum/value.size();
            wcd+=tmpSum;

        }

        bcd=bcd/(dataNum*centers.size());

        double sf=1-(1/(Math.exp(Math.exp(bcd-wcd))));

        return centers.size()+","+rare+","+sf+","+bcd+","+wcd+"\n";
    }


    public static ArrayList<Double> getDatacenters(HashMap<Integer,ArrayList<ArrayList<Double>>> data){

        ArrayList<Double> retValue=new ArrayList<Double>();
        int num=0;
        for (int i = 0; i <48 ; i++) {
            retValue.add(0.0);
        }
        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            ArrayList<ArrayList<Double>> value = entry.getValue();
            num+=value.size();
            for (int i = 0; i < value.size(); i++) {
                ArrayList<Double> tmpData=value.get(i);
                for (int j = 0; j < tmpData.size(); j++) {
                    retValue.set(j,retValue.get(j)+tmpData.get(j));
                }
            }
        }

        for (int i = 0; i < 48; i++) {
            retValue.set(i,retValue.get(i)/num);
        }
        return retValue;
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //String dataPath=args[0];
        //String centerPath=args[1];
        //int win=Integer.parseInt(args[2]);
        //String savePath=args[2];
        //String dataSetPath=args[3];

        //String dataSetPath="/sdb/kongdefei/hadoop/community_detect/clustering/portData20150706_0906";
        String data_path="/sdb/kongdefei/hadoop/community_detect/ex_20190409/";

        SF_EU sf=new SF_EU();

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"CFSFDP/CFSFDP_"+i+"/data",data_path+"CFSFDP/CFSFDP_"+i+"/centers/centers.txt");
            System.out.println(data_path+"CFSFDP");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"CFSFDP_0409_sf.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/data",data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/centers/centers.txt");
            System.out.println(data_path+"AgglomerativeClustering");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"AgglomerativeClustering_0409_sf.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"GaussianMixture/GaussianMixture_"+i+"/data",data_path+"GaussianMixture/GaussianMixture_"+i+"/centers/centers.txt");
            System.out.println(data_path+"GaussianMixture");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"GaussianMixture_0409_sf.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"KMeans/KMeans_"+i+"/data",data_path+"KMeans/KMeans_"+i+"/centers/centers.txt");
            System.out.println(data_path+"KMeans");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"KMeans_0409_sf.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"SpectralClustering/SpectralClustering_"+i+"/data",data_path+"SpectralClustering/SpectralClustering_"+i+"/centers/centers.txt");
            System.out.println(data_path+"SpectralClustering");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"SpectralClustering_0409_sf.txt");
        }

/*
        for (int i = 0; i < numList.size(); i++) {
            String w=di.evaluation(data_k_Path+"kmediods_"+numList.get(i)+"/data",data_k_Path+"kmediods_"+numList.get(i)+"/centers/centers.txt",win);
            //System.out.println(dataPath);
            System.out.print(w);
            FileWrite.WriteTxt(w,savePath_k);
        }
        */
        /*
        String w=sf.evaluation(dataPath,centerPath);
        System.out.println(dataPath);
        System.out.print(w);
        FileWrite.WriteTxt(w,savePath);
        */
    }
}
