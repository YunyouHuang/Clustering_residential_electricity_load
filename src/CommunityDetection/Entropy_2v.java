package CommunityDetection;

import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by hyy on 2019/4/14.
 */
public class Entropy_2v {

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

        String data_path="/sdb/kongdefei/hadoop/community_detect/ex_20190409/";


        for (int i = 0; i < 61; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_path+"CFSFDP/CFSFDP_"+i+"/data",0,0.0);
            System.out.println(data_path+"CFSFDP");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"CFSFDP_0409_entropy.txt");
        }

        for (int i = 0; i < 61; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/data",0,0.0);
            System.out.println(data_path+"AgglomerativeClustering");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"AgglomerativeClustering_0409_entropy.txt");
        }

        for (int i = 0; i < 61; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_path+"GaussianMixture/GaussianMixture_"+i+"/data",0,0.0);
            System.out.println(data_path+"GaussianMixture");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"GaussianMixture_0409_entropy.txt");
        }

        for (int i = 0; i < 61; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_path+"KMeans/KMeans_"+i+"/data",0,0.0);
            System.out.println(data_path+"KMeans");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"KMeans_0409_entropy.txt");
        }

        for (int i = 0; i < 61; i++) {
            Entropy di=new Entropy();
            String w=di.evaluation(data_path+"SpectralClustering/SpectralClustering_"+i+"/data",0,0.0);
            System.out.println(data_path+"SpectralClustering");
            System.out.print(w);
            FileWrite.WriteTxt(w,data_path+"SpectralClustering_0409_entropy.txt");
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
