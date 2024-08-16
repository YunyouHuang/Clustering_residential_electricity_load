package CommunityDetection;

import com.Data.ElectricProfile;
import com.FileOperate.FileWrite;
import com.Similarity.Distance;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by hyy on 2019/4/10.
 */
public class CFSFDP {

    private double[][] distanceMatrix;//=Matrix.Factory.zeros(a.size(),b.size());
    private HashMap<String,Integer> idMap=new HashMap<String, Integer>();
    private HashMap<Integer,String> index_Map=new HashMap<Integer, String>();
    private double arg_distance=0;
    private int num=0;
    private ArrayList<String> dataset=new ArrayList<String>();


    public CFSFDP(String dataPath){
        Distance dist=new Distance();
        ArrayList<ArrayList<Double>> data=getData_dataSet(dataPath);
        System.out.println("Load the data, complete !");
        int len=data.size();
        num=len;
        distanceMatrix=new double[len][len];
        double sum_distance=0;
        for (int i = 0; i < len; i++) {
            distanceMatrix[i][i]=0;
        }
        for (int i = 0; i <len ; i++) {
            for (int j = i+1; j < len; j++) {
                double tmpDis=dist.getEuclideanDistance(data.get(i),data.get(j));
                distanceMatrix[i][j]=tmpDis;
                distanceMatrix[j][i]=tmpDis;
                sum_distance+=tmpDis;
            }
        }
        arg_distance=sum_distance/(0.5*len*len);
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
                            dataset.add(tmpStr[1]);
                            retValue.add(tmpEle.getPoints());
                            String key=tmpEle.getID()+"-"+tmpEle.getDataDate();
                            idMap.put(key,index);
                            index_Map.put(index,key);
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

    public HashMap<String,Integer> get_dense(){

        HashMap<String,Integer> retValue=new HashMap<String, Integer>();

        boolean flag=true;
        double ner=0.5*arg_distance;
        double old_rate=0.5;
        //int k=0;
        while (flag){
            //flag=false;
            //ArrayList<Integer> dense=new ArrayList<Integer>();
            int[] dense=new int[num];
            for (int i = 0; i < num; i++) {
                dense[i]=1;
            }
            for (int i = 0; i < num; i++) {
                for (int j = i+1; j < num; j++) {
                    if (distanceMatrix[i][j]<ner){
                        dense[i]=dense[i]+1;
                        dense[j]=dense[j]+1;
                    }
                }
            }

            int sum_ner=0;
            for (int i = 0; i < num; i++) {
                sum_ner+=dense[i];
            }
            double avg_ner=((double) sum_ner)/num;

            double rate_ner=avg_ner/num;

            if ((rate_ner>0.01)&&(rate_ner<0.02)){
                System.out.println("Current rate :"+old_rate);
                System.out.println("Current ner_rate :"+rate_ner);
                //flag=false;
                for (int i = 0; i < num; i++) {
                    retValue.put(index_Map.get(i),dense[i]);
                }
                return retValue;
            }
            else {
                System.out.println("A new round ! *************************************");
                System.out.println("Previous rate :"+old_rate);
                System.out.println("Current ner_rate :"+rate_ner);
                Scanner sc = new Scanner(System.in);
                System.out.println("Entry rate : ");
                double rate = sc.nextDouble();
                ner=rate*arg_distance;
                old_rate=rate;
            }
        }
        return null;
    }

    public HashMap<String,Double> get_dist(HashMap<String,Integer> dense){
        HashMap<String,Double> retValue=new HashMap<String, Double>();
        //HashMap<String,Integer> dense=get_dense();

        for (Map.Entry<String, Integer> entry : dense.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();

            double best_dist=Double.MAX_VALUE;

            for (Map.Entry<String,Integer> entry1:dense.entrySet()){
                String key1=entry1.getKey();
                int value1=entry1.getValue();

                if ((!key.equals(key1))&&(value1>value)){
                    double tmp=distanceMatrix[idMap.get(key)][idMap.get(key1)];
                    if (tmp<best_dist){
                        best_dist=tmp;
                    }
                }
            }
            retValue.put(key,best_dist);
        }
        return retValue;
    }

    public static void main (String[] args) {
        // TODO Auto-generated method stub
        //String dataSetPath = "/sdb/kongdefei/hadoop/community_detect/clustering/portData20150706_0906";
        //String data_path = "/sdb/kongdefei/hadoop/community_detect/ex_20190409/CFSFDP/";
        String dataSetPath = args[0];
        String data_path = args[1];

        CFSFDP di = new CFSFDP(dataSetPath);

        HashMap<String, Integer> dense = di.get_dense();
        HashMap<String, Double> min_dist = di.get_dist(dense);

        for (Map.Entry<String, Double> entry : min_dist.entrySet()) {
            String key = entry.getKey();
            double value = entry.getValue();
            int tmp_dense = dense.get(key);
            String wt = key + "," + tmp_dense + "," + value + "\n";
            FileWrite.WriteTxt(wt, data_path + "dense_distance.txt");
        }

        while (true) {
            System.out.println("A new round ! *************************************");
            System.out.println("Please input dense and distance!");
            Scanner sc = new Scanner(System.in);
            //System.out.println("Entry rate : ");
            String line = sc.nextLine();
            String[] tmp_line = line.trim().split(",");

            if (tmp_line.length==2){
                boolean isvail=true;
                try {
                    Double.valueOf(tmp_line[1]);
                }catch (NumberFormatException e){
                    isvail=false;
                }

                try {
                    Integer.valueOf(tmp_line[0]);
                }catch (NumberFormatException e){
                    isvail=false;
                }

                if (isvail){
                    int dense_value = Integer.parseInt(tmp_line[0]);
                    double dist_value = Double.parseDouble(tmp_line[1]);

                    ArrayList<String> centers = new ArrayList<String>();
                    for (Map.Entry<String, Double> entry : min_dist.entrySet()) {
                        String key = entry.getKey();
                        double value = entry.getValue();
                        int tmp_dense = dense.get(key);
                        if ((value > dist_value) && (tmp_dense > dense_value)) {
                            centers.add(key);
                        }
                    }
                    System.out.println("The number of centers : " + centers.size());
                    System.out.println(" Is saving the result ?");
                    String flag_str = sc.nextLine();

                    boolean is_s_vail=true;
                    try {
                        Integer.valueOf(flag_str);
                    }catch (NumberFormatException e){
                        is_s_vail=false;
                    }

                    if (is_s_vail){
                        int flag=Integer.parseInt(flag_str);
                        if (flag == 1) {
                            System.out.println(" Please input file index !");
                            int index_file = sc.nextInt();

                            for (Map.Entry<String, Double> entry : min_dist.entrySet()) {
                                String key = entry.getKey();

                                double best_dist = Double.MAX_VALUE;
                                int best_index = -1;
                                for (int i = 0; i < centers.size(); i++) {
                                    double tmp_dist = di.distanceMatrix[di.idMap.get(key)][di.idMap.get(centers.get(i))];

                                    if (tmp_dist<best_dist) {
                                        best_dist = tmp_dist;
                                        best_index = i;
                                    }
                                }
                                String wt = "(" + best_index + "," + di.dataset.get(di.idMap.get(key)) + ")\n";
                                FileWrite.WriteTxt(wt, data_path + "CFSFDP_" + index_file + "/data/data.txt");
                            }

                            for (int i = 0; i < centers.size(); i++) {
                                String wt = i + ",[" + di.dataset.get(di.idMap.get(centers.get(i))).split(",", 8)[7] + "]\n";
                                FileWrite.WriteTxt(wt, data_path + "CFSFDP_" + index_file + "/centers/centers.txt");
                                FileWrite.WriteTxt(di.dataset.get(di.idMap.get(centers.get(i))), data_path + "CFSFDP_" + index_file + "/backup/centers.txt");
                            }
                        }
                    }
                }

            }

        }
    }
}
