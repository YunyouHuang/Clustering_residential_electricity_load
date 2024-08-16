package CommunityDetection;

import JavaTuple.Tulpe3IArray;
import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hyy on 2019/4/9.
 */
public class DBI_EU {

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

    public static String evaluation(String dataPath,String centerPath){
        HashMap<Integer,ArrayList<ArrayList<Double>>> data=getData(dataPath);
        //HashMap<Integer,ArrayList<Double>> centers=getCenters(centerPath);
        HashMap<Integer,ArrayList<Double>> centers=clearCenters(data,getCenters(centerPath));

        HashMap<Integer,Double> var=new HashMap<Integer,Double>();

        Distance dist=new Distance();
        int rare=0;


        for (Map.Entry<Integer, ArrayList<ArrayList<Double>>> entry : data.entrySet()) {
            Integer key = entry.getKey();
            ArrayList<ArrayList<Double>> value = entry.getValue();
            //retString+="  node\n  [\n    id "+value+"\n    label \""+key+"\"\n  ]\n";
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
            var.put(key,tmpSum/value.size());

        }


        HashMap<Integer,HashMap<Integer,Double>> cDistance=new HashMap<Integer, HashMap<Integer, Double>>();
        ArrayList<Tulpe3IArray> tmpCenters=new ArrayList<Tulpe3IArray>();
        for (Map.Entry<Integer, ArrayList<Double>> entry : centers.entrySet()) {
            int key = entry.getKey();
            ArrayList<Double> value=entry.getValue();
            tmpCenters.add(new Tulpe3IArray(key,value));
        }

        for (int i = 0; i < tmpCenters.size(); i++) {
            int key = tmpCenters.get(i).get_1();
            for (int j = i+1; j < tmpCenters.size(); j++) {
                int key2 = tmpCenters.get(j).get_1();
                if (key!=key2){
                    ArrayList<Double> value;
                    ArrayList<Double> value2;
                    if (data.get(key).size()>1){
                        value = tmpCenters.get(i).get_2();
                    }else {
                        value=data.get(key).get(0);
                    }

                    if (data.get(key2).size()>1){
                        value2 = tmpCenters.get(j).get_2();
                    }else {
                        value2=data.get(key2).get(0);
                    }

                    double tmpDist=dist.getEuclideanDistance(value,value2);
                    if (!cDistance.containsKey(key)){
                        cDistance.put(key,new HashMap<Integer, Double>());
                    }
                    cDistance.get(key).put(key2,tmpDist);

                    if (!cDistance.containsKey(key2)){
                        cDistance.put(key2,new HashMap<Integer, Double>());
                    }
                    cDistance.get(key2).put(key,tmpDist);
                }
            }
        }

        double bdi=0;
        for (int i = 0; i < tmpCenters.size(); i++) {
            int key=tmpCenters.get(i).get_1();
            double bestValue=0;
            for (int j = 0; j < tmpCenters.size(); j++) {
                if (i!=j){
                    int key2=tmpCenters.get(j).get_1();

                    double tmpValue=(var.get(key)+var.get(key2))/cDistance.get(key).get(key2);

                    if (bestValue<tmpValue){
                        bestValue=tmpValue;
                    }
                }
            }
            bdi+=bestValue;

        }

        return centers.size()+","+rare+","+bdi/centers.size()+"\n";
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String dataPath=args[0];
        String centerPath=args[1];
        //int win=Integer.parseInt(args[2]);
        String savePath=args[2];

        String w=evaluation(dataPath,centerPath);
        System.out.println(dataPath);
        System.out.print(w);
        FileWrite.WriteTxt(w,savePath);
    }
}
