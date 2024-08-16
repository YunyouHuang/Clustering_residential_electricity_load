package CommunityDetection;

import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hyy on 2018/6/26.
 */
public class Evaluate {


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


    public static String evaluation(String dataPath,String centerPath,int dtw_win){
        HashMap<Integer,ArrayList<ArrayList<Double>>> data=getData(dataPath);
        HashMap<Integer,ArrayList<Double>> centers=getCenters(centerPath);
        Distance dist=new Distance();
        double WC=0;
        double WB=0;
        int rare=0;


        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();
            //retString+="  node\n  [\n    id "+value+"\n    label \""+key+"\"\n  ]\n";
            if (value.size()>1){
                ArrayList<Double> center=centers.get(key);
                for (int i = 0; i < value.size(); i++) {
                    ArrayList<Double> tmpData=value.get(i);
                    WC+=dist.getDTWDistanceForDouble(center,tmpData,dtw_win);
                }
            }else {
                rare++;
            }

        }


        for (Map.Entry<Integer, ArrayList<Double>> entry : centers.entrySet()) {
            int key = entry.getKey();
            for (Map.Entry<Integer, ArrayList<Double>> entry2 : centers.entrySet()) {
                int key2 = entry2.getKey();
                if (key!=key2){
                    ArrayList<Double> value;
                    ArrayList<Double> value2;
                    if (data.get(key).size()>1){
                        value = entry.getValue();
                    }else {
                        value=data.get(key).get(0);
                    }

                    if (data.get(key2).size()>1){
                        value2 = entry2.getValue();
                    }else {
                        value2=data.get(key2).get(0);
                    }

                    WB+=dist.getDTWDistanceForDouble(value,value2,dtw_win);
                }
            }

        }
        WB=WB*0.5;

        return centers.size()+","+rare+","+WC+","+WB+","+(WC/WB)+"\n";
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String dataPath=args[0];
        String centerPath=args[1];
        int win=Integer.parseInt(args[2]);
        String savePath=args[3];

        String w=evaluation(dataPath,centerPath,win);
        System.out.println(dataPath);
        System.out.print(w);
        FileWrite.WriteTxt(w,savePath);
    }

}
