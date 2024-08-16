package CommunityDetection;

import JavaTuple.Tulpe2SA;
import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hyy on 2018/7/19.
 */
public class COP {
    private double[][] distanceMatrix;//=Matrix.Factory.zeros(a.size(),b.size());
    private int data_N;
    private HashMap<String,Integer> idMap=new HashMap<String, Integer>();

    public COP(String dataPath,int win){
        Distance dist=new Distance();
        ArrayList<ArrayList<Double>> data=getData_dataSet(dataPath);
        System.out.println("Load the data, complete !");
        int len=data.size();
        data_N=len;
        distanceMatrix=new double[len][len];
        for (int i = 0; i < len; i++) {
            distanceMatrix[i][i]=0;
        }
        for (int i = 0; i <len ; i++) {
            for (int j = i+1; j < len; j++) {
                double tmpDis=dist.getDTWDistanceForDouble(data.get(i),data.get(j),win);
                distanceMatrix[i][j]=tmpDis;
                distanceMatrix[j][i]=tmpDis;
            }
        }
        System.out.println("Create the distance Matrix, complete !");

    }

    public ArrayList<ArrayList<Double>> getData_dataSet(String path){

        int index=0;
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
                            String key=tmpEle.getID()+"-"+tmpEle.getDataDate();
                            idMap.put(key,index);
                            index++;
                            //dataSet.add(tmpStr[1]);
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

    public HashMap<Integer,ArrayList<Tulpe2SA>> getData(String path){

        HashMap<Integer,ArrayList<Tulpe2SA>> retValue=new HashMap<Integer, ArrayList<Tulpe2SA>>();
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
                                retValue.put(key,new ArrayList<Tulpe2SA>());
                            }
                            String key_id=tmpEle.getID()+"-"+tmpEle.getDataDate();
                            retValue.get(key).add(new Tulpe2SA(key_id,tmpEle.getPoints()));
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


    public HashMap<Integer,ArrayList<Double>> getCenters(String path){

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

    public static HashMap<Integer,ArrayList<Double>> clearCenters(HashMap<Integer,ArrayList<Tulpe2SA>> data,HashMap<Integer,ArrayList<Double>> centers){
        HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        for (Map.Entry<Integer, ArrayList<Double>> entry : centers.entrySet()) {
            int key=entry.getKey();
            if (data.containsKey(key)){
                retValue.put(key,entry.getValue());
            }
        }
        return retValue;
    }

    public String evaluation(String dataPath,String centerPath,int dtw_win){
        HashMap<Integer,ArrayList<Tulpe2SA>> data=getData(dataPath);
        //HashMap<Integer,ArrayList<Double>> centers=getCenters(centerPath);
        HashMap<Integer,ArrayList<Double>> centers=clearCenters(data,getCenters(centerPath));
        Distance dist=new Distance();

        int rare=0;
        double cop=0;
        int dataSetNum=0;
        for (Map.Entry<Integer, ArrayList<Tulpe2SA>> entry : data.entrySet()) {
            int key=entry.getKey();
            ArrayList<Tulpe2SA> value = entry.getValue();
            dataSetNum+=value.size();

            if (value.size()<2){
                rare++;
            }

            double intra=0;
            if (value.size()>1){
                for (int i = 0; i < value.size(); i++) {
                    //Tulpe2SA x=value.get(i);
                    ArrayList<Double> tmpCenter=centers.get(key);
                    intra+=dist.getDTWDistanceForDouble(tmpCenter,value.get(i).get_2(),dtw_win);
                }
                intra=intra/value.size();
            }

            double inter=Double.MAX_VALUE;
            for (Map.Entry<Integer, ArrayList<Tulpe2SA>> entry_y : data.entrySet()) {
                int key_2=entry_y.getKey();
                if (key!=key_2){
                    ArrayList<Tulpe2SA> value_y=entry_y.getValue();
                    for (int j = 0; j < value_y.size(); j++) {
                        double tmp_inter=0;
                        for (int i = 0; i < value.size(); i++) {
                            double tmpDist=distanceMatrix[idMap.get(value_y.get(j).get_1())][idMap.get(value.get(i).get_1())];
                            if (tmp_inter<tmpDist){
                                tmp_inter=tmpDist;
                            }
                        }
                        if (inter>tmp_inter){
                            inter=tmp_inter;
                        }
                    }
                }
            }
            cop+=((intra/inter)*value.size());
        }

        return centers.size()+","+rare+","+(cop/dataSetNum)+"\n";
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String data_cd_Path=args[0];
        String data_k_Path=args[1];
        int win=Integer.parseInt(args[2]);
        String savePath_cd=args[3];
        String savePath_k=args[4];
        String centerNumPath=args[5];
        String dataSetPath=args[6];


        String rate="10,0975,095,0925,09" +
                ",0875,085,0825,08" +
                ",0775,075,0725,07" +
                ",0675,065,0625,06" +
                ",0575,055,0525,05" +
                ",0475,045,0425,04" +
                ",0375,035,0325,03";

        String rate_2="1.000,0.995,0.990,0.985,0.980,0.975,0.970,0.965,0.960,0.955,0.950,0.945,0.940,0.935,0.930,0.925,0.920,0.915,0.910,0.905,0.900,0.895,0.890,0.885,0.880,0.875,0.870,0.865,0.860,0.855,0.850,0.845,0.840,0.835,0.830,0.825,0.820,0.815,0.810,0.805,0.800,0.795,0.790,0.785,0.780,0.775,0.770,0.765,0.760,0.755,0.750,0.745,0.740,0.735,0.730,0.725,0.720,0.715,0.710,0.705,0.700";

        String[] rateArray=rate_2.split(",");

        //ArrayList<Integer> numList=getCentersNum(centerNumPath);

        COP di=new COP(dataSetPath,win);

        for (int i = 0; i < rateArray.length; i++) {
            String w=di.evaluation(data_cd_Path+"r_3_075_1_1_"+rateArray[i],data_cd_Path+"centers/r_3_075_1_1_"+rateArray[i]+"_3_centers/centers.txt",win);
            //System.out.println(dataPath);
            System.out.print(w);
            FileWrite.WriteTxt(w,savePath_cd);
        }

        for (int i = 0; i < 61; i++) {
            String w=di.evaluation(data_k_Path+"kmediods_"+i+"/data",data_k_Path+"kmediods_"+i+"/centers/centers.txt",win);
            //System.out.println(dataPath);
            System.out.print(w);
            FileWrite.WriteTxt(w,savePath_k);
        }
/*
        for (int i = 0; i < numList.size(); i++) {
            String w=di.evaluation(data_k_Path+"kmediods_"+numList.get(i)+"/data",data_k_Path+"kmediods_"+numList.get(i)+"/centers/centers.txt",win);
            //System.out.println(dataPath);
            System.out.print(w);
            FileWrite.WriteTxt(w,savePath_k);
        }
*/

    }
}
