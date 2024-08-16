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
 * Created by hyy on 2020/1/17.
 */
public class ErrorRate {

    public static ArrayList<HashSet<Integer>> getData(String path){

        ArrayList<HashSet<Integer>> retValue=new ArrayList<HashSet<Integer>>();

        HashMap<Integer,HashSet<Integer>> tmpRet=new HashMap<Integer, HashSet<Integer>>();
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

                            if (!tmpRet.containsKey(key)){
                                tmpRet.put(key,new HashSet<Integer>());
                            }
                            tmpRet.get(key).add((int)tmpEle.getID());
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

        for (Map.Entry<Integer, HashSet<Integer>> entry : tmpRet.entrySet()) {
            retValue.add(entry.getValue());
        }
        return retValue;
    }

    public static ArrayList<HashSet<Integer>> getLable(String path){

        //XYSeriesCollection sc=new XYSeriesCollection();
        //HashMap<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        ArrayList<HashSet<Integer>> retValue = new ArrayList<HashSet<Integer>>();
        for (int i = 0; i < 7; i++) {
            retValue.add(new HashSet<Integer>());
        }

        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){

                line=line.trim();
                String[] tmpStr=line.split(",");
                int userID=Integer.parseInt(tmpStr[0]);
                int c_lable=Integer.parseInt(tmpStr[1]);

                retValue.get(c_lable-1).add(userID);

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

    public static String evaluation(String dataPath,String centerPath){
        ArrayList<HashSet<Integer>> data=getData(dataPath);
        ArrayList<HashSet<Integer>> lable=getLable(centerPath);

        HashSet<Integer> index_set =new HashSet<>();
        int error_count=0;
        int count=0;

        for (int i = 0; i < data.size(); i++) {
            count+=data.get(i).size();
            int error=0;
            int best_index=-1;
            for (int j = 0; j < lable.size(); j++) {
                if (!index_set.contains(j)){
                    HashSet<Integer> tmp=new HashSet<Integer>();
                    tmp.clear();
                    tmp.addAll(data.get(i));
                    tmp.removeAll(lable.get(j));
                    int tmp_error=tmp.size();
                    if (tmp_error>error){
                        error=tmp_error;
                        best_index=j;
                    }
                }
            }
            index_set.add(best_index);
            error_count+=error;
        }
        System.out.print("Error rate : "+error_count/count);

        return "";
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String dataPath=args[0];
        String centerPath=args[1];

        String w=evaluation(dataPath,centerPath);
    }
}
