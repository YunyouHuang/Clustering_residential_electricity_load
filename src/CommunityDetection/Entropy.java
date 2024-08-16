package CommunityDetection;

import JavaTuple.Tulpe3IArray;
import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by hyy on 2018/7/26.
 */
public class Entropy {

    private HashMap<Integer,HashMap<Integer,Integer>> userMap=new HashMap<Integer, HashMap<Integer, Integer>>();
    private HashMap<Integer,Integer> userNUm=new HashMap<Integer, Integer>();
    private HashSet<Integer> clusterId=new HashSet<>();

    public void loadData(String path){

        //HashMap<Integer,ArrayList<ArrayList<Double>>> retValue=new HashMap<Integer, ArrayList<ArrayList<Double>>>();
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
                            clusterId.add(key);
                            int userId=(int)tmpEle.getID();
                            if (!userNUm.containsKey(userId)){
                                userNUm.put(userId,1);
                            }else {
                                userNUm.put(userId,userNUm.get(userId)+1);
                            }

                            if (!userMap.containsKey(userId)){
                                userMap.put(userId,new HashMap<Integer, Integer>());
                            }

                            if (!userMap.get(userId).containsKey(key)){
                                userMap.get(userId).put(key,1);
                            }else {
                                userMap.get(userId).put(key,userMap.get(userId).get(key)+1);
                            }
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
        //return retValue;
    }

    public String evaluation(String dataPath, int log_flag, double log_base){
        loadData(dataPath);
        double value_s=0;
        for (Map.Entry<Integer,HashMap<Integer,Integer>> entry : userMap.entrySet()) {
            int userID = entry.getKey();

            HashMap<Integer,Integer> value = entry.getValue();
            double tmpS=0;
            for (Map.Entry<Integer,Integer> entry_value : value.entrySet()) {

                double p_k=entry_value.getValue()/(userNUm.get(userID)*1.0);

                if (log_flag==0){
                    tmpS+=p_k*Math.log(p_k);
                }else if (log_flag==1){
                    tmpS+=p_k*(Math.log(p_k)/Math.log(log_base));
                }else {
                    if (value.size()==1){
                        tmpS+=0;
                    }else {
                        tmpS+=p_k*(Math.log(p_k)/Math.log(value.size()));
                    }

                }

            }
            tmpS=-tmpS;
            value_s+=tmpS;
        }
        value_s=value_s/userNUm.size();

        return clusterId.size()+","+value_s+"\n";
    }


    public static void main(String[] args) {

        String data_cd_Path=args[0];
        String data_k_Path=args[1];
        //int win=Integer.parseInt(args[2]);
        int log_flag=Integer.parseInt(args[2]);
        double log_base=Double.parseDouble(args[3]);
        String savePath_cd=args[4];
        String savePath_k=args[5];
        //String centerNumPath=args[5];
        //String dataSetPath=args[6];


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



        for (int i = 0; i < rateArray.length; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_cd_Path+"r_3_075_1_1_"+rateArray[i],log_flag,log_base);
            //System.out.println(dataPath);
            System.out.print(w);
            FileWrite.WriteTxt(w,savePath_cd);
        }

        for (int i = 0; i < 61; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_k_Path+"kmediods_"+i+"/data",log_flag,log_base);
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
