package CommunityDetection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class eleCount {

	public static int getmark(ArrayList<String> keyArry,ArrayList<Integer> valueArry){
		int sum=0;
		for (int i = 0; i < valueArry.size(); i++) {
			sum+=valueArry.get(i);
		}
		double randomForSelect=Math.random();
		int clu=0;
		int bestIndex=-1;
		for (int i = 0; i < valueArry.size(); i++) {
			clu+=valueArry.get(i);
			double tmpMark=(double) clu/(double) sum;
			if (randomForSelect<=tmpMark){
				bestIndex=Integer.parseInt(keyArry.get(i));
				break;
			}
		}
		return bestIndex;
	}

	public static HashMap<String, Integer> getNum(String pathString){
		String filepath=pathString;
		File file = new File(filepath);
		if (!file.isDirectory()) {
			System.out.println("文件！");
			System.out.println("path=" + file.getPath());
			System.out.println("absolutepath=" + file.getAbsolutePath());
			System.out.println("name=" + file.getName());
			return new HashMap<String, Integer>();
		} else if (file.isDirectory()) {
			System.out.println("文件夹！");
			String[] filelist = file.list();
			String retValueString="";
			HashMap<String, Integer> kkHashMap=new HashMap<String, Integer>();
			for (int i = 0; i < filelist.length; i++) {
				File readfile = new File(filepath + "/" + filelist[i]);
				if (!readfile.isDirectory()) {
					BufferedReader reader=null;
					try {
						reader=new BufferedReader(new InputStreamReader(new FileInputStream(readfile),"UTF-8"));
						String line=null;
						while ((line=reader.readLine())!=null){
							String keyString;
							if (line.contains("(")) {
								keyString=line.substring(1,line.length()-1).split(",",2)[0];
							}else{
								keyString=line.split(",",2)[1];
							}

							//System.out.println(keyString);
							if (kkHashMap.containsKey(keyString)) {
								int value=kkHashMap.get(keyString)+1;
								kkHashMap.put(keyString, value);
							}else {
								kkHashMap.put(keyString, 1);
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


			ArrayList<String> keyArry=new ArrayList<String>();
			ArrayList<Integer> valueArry=new ArrayList<Integer>();
			Iterator iter = kkHashMap.entrySet().iterator();
			String centersStr="";
			int centerIndex=0;
			while (iter.hasNext()) {
				Map.Entry<String,Integer> entry = (Map.Entry<String,Integer>) iter.next();
				int val = entry.getValue();
				String keyString=entry.getKey();
				//System.out.print(keyString+"   "+val+"\n");

				ArrayList<String> tmpKeyArry=new ArrayList<String>();
				ArrayList<Integer> tmpValueArry=new ArrayList<Integer>();

				ArrayList<String> partKeyArry=new ArrayList<String>();
				ArrayList<Integer> partValueArry=new ArrayList<Integer>();

				for (int i = 0; i < keyArry.size(); i++) {
					tmpKeyArry.add(keyArry.get(i));
					tmpValueArry.add(valueArry.get(i));
				}

				keyArry.clear();
				valueArry.clear();
				int insert=-1;
				if (tmpKeyArry.size()==0) {
					insert=0;
				}else {
					for (int i = 0; i < tmpValueArry.size(); i++) {
						if (tmpValueArry.get(i)>val) {
							insert=i;
							for (int j = i; j < tmpKeyArry.size(); j++) {
								partKeyArry.add(tmpKeyArry.get(j));
								partValueArry.add(tmpValueArry.get(j));
							}
							break;
						}
					}
				}

				if (partKeyArry.size()==0) {
					for (int i = 0; i < tmpKeyArry.size(); i++) {
						keyArry.add(tmpKeyArry.get(i));
						valueArry.add(tmpValueArry.get(i));
					}
					keyArry.add(keyString);
					valueArry.add(val);
				}else {
					for (int i = 0; i < insert; i++) {
						keyArry.add(tmpKeyArry.get(i));
						valueArry.add(tmpValueArry.get(i));
					}
					keyArry.add(keyString);
					valueArry.add(val);
					for (int i = 0; i < partKeyArry.size(); i++) {
						keyArry.add(partKeyArry.get(i));
						valueArry.add(partValueArry.get(i));
					}

				}

			}

			System.out.print("==============================="+"\n");
			HashMap<String, Integer> retValue=new HashMap<String, Integer>();
			int countRare=0;
			long loadCount=0;
			for (int i = 0; i < keyArry.size(); i++) {
				if (valueArry.get(i)<2) {
					countRare++;
					loadCount+=valueArry.get(i);
				}
				System.out.print(keyArry.get(i)+"   "+valueArry.get(i)+"\n");
				retValue.put(keyArry.get(i), valueArry.get(i));
			}
			System.out.println("************************************************");
			System.out.println(keyArry.size());
			System.out.println(countRare);
			System.out.println(loadCount);
		        /*
		        String reString="";
		        for (int i = 0; i < 35100; i++) {
					long userId=100000+i;
					reString+=userId+","+getmark(keyArry, valueArry)+"\n";
					if (i%20000==0) {
						System.out.println("generate user :"+i);
					}
				}
		        new eva().WriteTxt(reString, "G:\\test\\20170519\\users.txt");
		        */
			return retValue;
		}
		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		HashMap<String, Integer> or=getNum(args[0]);

    
	}

}
