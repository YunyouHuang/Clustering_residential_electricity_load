package CommunityDetection;

import com.Data.DataOperation;
import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.*;

/**
 * Created by hyy on 2019/4/12.
 */
public class S_Dbw_EU {

    private double datasetvar=0;
    private boolean isFlag=false;

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


    public static HashMap<Integer,ArrayList<Double>> getCentersVar(HashMap<Integer,ArrayList<ArrayList<Double>>> data,HashMap<Integer,ArrayList<Double>> centers){

        HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();
            //ArrayList<Double> center=centers.get(key);

            //retString+="  node\n  [\n    id "+value+"\n    label \""+key+"\"\n  ]\n";
            //double tmpSum=0;
            if (value.size()>1){
                ArrayList<Double> center=centers.get(key);
                ArrayList<Double> sum_cVar=new ArrayList<Double>();
                for (int i = 0; i < 96; i++) {
                    sum_cVar.add(0.0);
                }
                for (int i = 0; i < value.size(); i++) {
                    ArrayList<Double> tmpData=value.get(i);
                    ArrayList<Double> tmpDataVar= DataOperation.arrayPow(center,tmpData);
                    for (int j = 0; j < tmpDataVar.size(); j++) {
                        sum_cVar.set(j,sum_cVar.get(j)+tmpDataVar.get(j));
                    }
                    //tmpSum+=dist.getDTWDistanceForDouble(center,tmpData,dtw_win);
                }
                ArrayList<Double> cVar=DataOperation.ArrayDivid_2v(sum_cVar,value.size());
                retValue.put(key,cVar);
            }
            else if (value.size()==1){
                retValue.put(key,new ArrayList<Double>());
            }

        }
        return retValue;
    }

    //获取所有类的平均方差
    public static double getMeansVar(HashMap<Integer,ArrayList<Double>> var){

        double retValue=0;
        for (Map.Entry<Integer, ArrayList<Double>> entry : var.entrySet()) {
            ArrayList<Double> value = entry.getValue();

            if (value.size()>0){
                retValue+=DataOperation.getArrayModel(value);
            }
        }
        retValue=Math.sqrt(retValue)/var.size();
        return retValue;
    }

    //获取所有类的平均方差
    public static double getMeansVar_2v(HashMap<Integer,ArrayList<Double>> var){

        double retValue=0;
        for (Map.Entry<Integer, ArrayList<Double>> entry : var.entrySet()) {
            ArrayList<Double> value = entry.getValue();

            if (value.size()>0){
                retValue+=DataOperation.getArrayModel(value);
            }
        }
        retValue=retValue/var.size();
        return retValue;
    }

    public static ArrayList<Double> getDatacenters(HashMap<Integer,ArrayList<ArrayList<Double>>> data){

        ArrayList<Double> retValue=new ArrayList<Double>();
        int num=0;
        for (int i = 0; i <96 ; i++) {
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

        for (int i = 0; i < 96; i++) {
            retValue.set(i,retValue.get(i)/num);
        }
        return retValue;
    }

    public static double getDatasetVar(HashMap<Integer,ArrayList<ArrayList<Double>>> data, ArrayList<Double> center){

        ArrayList<Double> sum_cVar=new ArrayList<Double>();
        for (int i = 0; i < 96; i++) {
            sum_cVar.add(0.0);
        }

        int num=0;
        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            //Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();
            num+=value.size();
            for (int i = 0; i < value.size(); i++) {
                ArrayList<Double> tmpData=value.get(i);
                ArrayList<Double> tmpDataVar= DataOperation.arrayPow(center,tmpData);
                for (int j = 0; j < tmpDataVar.size(); j++) {
                    sum_cVar.set(j,sum_cVar.get(j)+tmpDataVar.get(j));
                }
                    //tmpSum+=dist.getDTWDistanceForDouble(center,tmpData,dtw_win);
            }
                //ArrayList<Double> cVar=DataOperation.ArrayDivid_2v(sum_cVar,value.size());
                //retValue.put(key,cVar);
        }
        return DataOperation.getArrayModel(DataOperation.ArrayDivid_2v(sum_cVar,num));

    }

    public String evaluation(String dataPath,String centerPath){
        HashMap<Integer,ArrayList<ArrayList<Double>>> data=getData(dataPath);
        HashMap<Integer,ArrayList<Double>> centers=getCenters(centerPath);

        if (!isFlag){
            datasetvar=getDatasetVar(data,getDatacenters(data));
            isFlag=true;
        }

        HashMap<Integer,ArrayList<Double>> centersVar=getCentersVar(data,centers);
        double stdev=getMeansVar(centersVar);

        HashMap<Integer,Integer> cDense=new HashMap<Integer, Integer>();
        Distance dist=new Distance();
        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();
            //retString+="  node\n  [\n    id "+value+"\n    label \""+key+"\"\n  ]\n";
            //double tmpSum=0;
            int dense_count=0;
            if (value.size()>1){
                ArrayList<Double> center=centers.get(key);
                for (int i = 0; i < value.size(); i++) {
                    ArrayList<Double> tmpData=value.get(i);
                    double tmp_dist=dist.getEuclideanDistance(center,tmpData);
                    if (tmp_dist<stdev){
                        //cDense.put(key,dense_count);
                        dense_count++;
                    }
                }
                cDense.put(key,dense_count);
            }else if (value.size()==1){
                cDense.put(key,1);
            }

        }

        double debs_bw=0;
        HashSet<String> keySet=new HashSet<String>();

        int rep=0;
        int newcount=0;
        for (Map.Entry<Integer, Integer> entry : cDense.entrySet()) {
            int key=entry.getKey();
            int value=entry.getValue();
            for (Map.Entry<Integer, Integer> entry_2 : cDense.entrySet()) {
                int key_2=entry_2.getKey();
                int value_2=entry_2.getValue();
                if (key!=key_2){
                    String key2key_1=key+"_"+key_2;
                    String key2key_2=key_2+"_"+key;

                    if (keySet.contains(key2key_1)||keySet.contains(key2key_2)){
                        rep+=1;
                    }else {
                        newcount+=1;
                        keySet.add(key2key_1);
                        keySet.add(key2key_2);

                        ArrayList<Double> mid_center=DataOperation.ArrayDivid_2v(DataOperation.arrayPluForDouble(centers.get(key),centers.get(key_2)),2);

                        ArrayList<ArrayList<Double>> valuei=data.get(key);
                        ArrayList<ArrayList<Double>> valuej=data.get(key_2);

                        int mid_count=0;
                        for (int i = 0; i < valuei.size(); i++) {
                            ArrayList<Double> tmpData=valuei.get(i);
                            double tmp_dist=dist.getEuclideanDistance(mid_center,tmpData);
                            if (tmp_dist<stdev){
                                mid_count+=1;
                            }
                        }
                        for (int i = 0; i < valuej.size(); i++) {
                            ArrayList<Double> tmpData=valuej.get(i);
                            double tmp_dist=dist.getEuclideanDistance(mid_center,tmpData);
                            if (tmp_dist<stdev){
                                mid_count+=1;
                            }
                        }

                        int max_fen=Math.max(value,value_2);
                        if (max_fen<=0){
                            max_fen=1;
                        }
                        debs_bw+=((double) mid_count)/max_fen;
                    }
                }
            }
        }

        System.out.println("rep count :  "+rep);
        System.out.println("new count :  "+newcount);
        debs_bw=debs_bw*2/(cDense.size()*(cDense.size()-1));


        double varCenters=getMeansVar_2v(centersVar);

        //double dataSetVar=doper.getArrayModel(variance(dataSet.map(new trans())));

        double scat=varCenters/(datasetvar*cDense.size());

        //return dens_bw+scat;
        //return new Tuple2<Double,Double>(dens_bw,scat);

        return centers.size()+","+(scat+debs_bw)+","+scat+","+debs_bw+"\n";
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

        S_Dbw_EU sf=new S_Dbw_EU();

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"CFSFDP/CFSFDP_"+i+"/data",data_path+"CFSFDP/CFSFDP_"+i+"/centers/centers.txt");
            System.out.println(data_path+"CFSFDP");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"CFSFDP_0409_S_Dbw.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/data",data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/centers/centers.txt");
            System.out.println(data_path+"AgglomerativeClustering");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"AgglomerativeClustering_0409_S_Dbw.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"GaussianMixture/GaussianMixture_"+i+"/data",data_path+"GaussianMixture/GaussianMixture_"+i+"/centers/centers.txt");
            System.out.println(data_path+"GaussianMixture");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"GaussianMixture_0409_S_Dbw.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"KMeans/KMeans_"+i+"/data",data_path+"KMeans/KMeans_"+i+"/centers/centers.txt");
            System.out.println(data_path+"KMeans");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"KMeans_0409_S_Dbw.txt");
        }

        for (int i = 0; i < 61; i++) {
            String w=sf.evaluation(data_path+"SpectralClustering/SpectralClustering_"+i+"/data",data_path+"SpectralClustering/SpectralClustering_"+i+"/centers/centers.txt");
            System.out.println(data_path+"SpectralClustering");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"SpectralClustering_0409_S_Dbw.txt");
        }
    }
}
