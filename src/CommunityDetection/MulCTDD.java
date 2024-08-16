package CommunityDetection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MulCTDD {

	public HashMap<Integer, HashMap<Integer, Float>> _keyEdgeSet=new HashMap<Integer, HashMap<Integer,Float>>();
	
	public HashMap<Integer, HashMap<Integer, Float>> _exKeyEdgeSet=new HashMap<Integer, HashMap<Integer,Float>>();
	
    public HashMap<Integer,HashMap<Integer, Float>> neigbhor=new HashMap<Integer, HashMap<Integer,Float>>();
    
    public ExecutorService exe;
    public int thrNum=1;
    
    public HashSet<Integer> getNode(HashMap<Integer, Float> data) {
		HashSet<Integer> retValue=new HashSet<Integer>();
        Iterator iter = data.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
            int key=entry.getKey();
            retValue.add(key);
        }
        return retValue;
	}

    public HashMap<Integer, Float> getNei(int x) {
    	HashMap<Integer, Float> _neighbor=new HashMap<Integer, Float>();
    	HashMap<Integer, Float> nei=neigbhor.get(x);
    	if (nei!=null) {
    		_neighbor.putAll(nei);
		}
    	return _neighbor;
	}
    
    public HashSet<Integer> getEN(int x,int y) {
    	HashMap<Integer, Float> xNeighbor=getNei(x);
    	HashMap<Integer, Float> yNeighbor=getNei(y);
    	
    	HashSet<Integer> xNeighborNodeSet=getNode(xNeighbor);
    	HashSet<Integer> yNeighborNodeSet=getNode(yNeighbor);
    	
    	xNeighborNodeSet.removeAll(yNeighborNodeSet);
    	if (xNeighborNodeSet.contains(x)) {
			xNeighborNodeSet.remove(x);
		}
    	
    	if (xNeighborNodeSet.contains(y)) {
			xNeighborNodeSet.remove(y);
		}
    	return xNeighborNodeSet;
	}
    
    public boolean isContainInEXEdge_bak(int x,int y) {
    	if (_exKeyEdgeSet.containsKey(x)) {
			if (_exKeyEdgeSet.get(x).containsKey(y)) {
				return true;
			}
		}
    	if (_exKeyEdgeSet.containsKey(y)) {
			if (_exKeyEdgeSet.get(y).containsKey(x)) {
				return true;
			}
		}
    	return false;
	}
    
    public boolean isContainInEXEdge(int x,int y,HashSet<String> exRecord) {
		String key1=x+"-"+y;
		if (exRecord.contains(key1)) {
			return true;
		}
		
		String key2=y+"-"+x;
		if (exRecord.contains(key2)) {
			return true;
		}
		
		return false;
	}

	//region
    /*
    public int calDistanceForEN(int x,int y,int edgeCount,int thrCount,
    		HashSet<String> exRecord,
    		InitialEdge iEdgeSet) throws InterruptedException {
    	int subCount=0;
    	HashSet<Integer> xEN=getEN(x, y);
    	HashSet<Integer> yEN=getEN(y, x);
    	for (Integer yNode : yEN) {
    		if(x!=yNode){
            	if (!isContainInEXEdge(x, yNode,exRecord)) {
                	if (thrCount>=thrNum) {
                		exe.shutdown();
                           while (true) {
                               if (exe.isTerminated()) {
                                    System.out.println("�߳�   "+(edgeCount-1)+" �����ˣ�");
                                    break;
                                }
                                Thread.sleep(200);
                            }
                            thrCount=0;
                            exe=null;
                            exe = Executors.newFixedThreadPool(thrNum+2);
            			}
                	MulCTDD md=new MulCTDD();
        			InitialThread tmpThr=md.new InitialThread("exEdgeThr "+edgeCount+"-"+subCount, iEdgeSet, neigbhor, 1, x, yNode, 0);
        			exe.execute(tmpThr);
        			exRecord.add(x+"-"+yNode);
        			subCount++;
        			thrCount++;
            		}	
    		}
		}
    	
    	for (Integer xNode : xEN) {
    		if (y!=xNode) {
    			if (!isContainInEXEdge(y, xNode,exRecord)) {
    				if (thrCount>=thrNum) {
    					exe.shutdown();
                        while (true) {
                            if (exe.isTerminated()) {
                                System.out.println("�߳�   "+(edgeCount-1)+" �����ˣ�");
                                break;
                            }
                            Thread.sleep(200);
                        }
                        thrCount=0;
                        exe=null;
                        exe = Executors.newFixedThreadPool(thrNum+2);
    				}
    				MulCTDD md=new MulCTDD();
    				InitialThread tmpThr=md.new InitialThread("exEdgeThr "+edgeCount+"-"+subCount, iEdgeSet, neigbhor, 1, xNode, y, 0);
    				exe.execute(tmpThr);
    				exRecord.add(y+"-"+xNode);
    				subCount++;
    				thrCount++;
    			}
			}
		}
    	return thrCount;
	}
    */
    //endregion
    
    public void loadingEdge(String path,int flag){
		//_start  ���رߣ�������нڵ���ھ�
		//HashMap<Integer, HashMap<Integer, Float>> keyEdgeSet=new HashMap<Integer, HashMap<Integer,Float>>();//Ȩ�ر����ݼ�
	    int count=0;
        File file = new File(path);
        if (file.isDirectory()) {
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
            	File readfile;
            	if (flag==0) {
            		readfile = new File(path + "/" + filelist[i]);
				}else {
					readfile = new File(path + "\\" + filelist[i]);
				}
                if (!readfile.isDirectory()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(new FileInputStream(readfile), "UTF-8"));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                        	String[] tmpStr=line.split(",");
                        	int x=Integer.parseInt(tmpStr[0]);
                        	int y=Integer.parseInt(tmpStr[1]);
                        	float weight=Float.parseFloat(tmpStr[2]);
                        	
        					if (!neigbhor.containsKey(x)) {
        						neigbhor.put(x, new HashMap<Integer, Float>());
        					}
        					neigbhor.get(x).put(y, weight);
        					if (!neigbhor.containsKey(y)) {
        						neigbhor.put(y, new HashMap<Integer, Float>());
        					}
        					neigbhor.get(y).put(x, weight);
        					
                        	if (!_keyEdgeSet.containsKey(x)) {
                        		_keyEdgeSet.put(x, new HashMap<Integer, Float>());
							}
                        	_keyEdgeSet.get(x).put(y, weight);
                        	count++;
                        	if (count%25000==0) {
								System.out.println("loading edge index   "+count);
							}
                        }
						reader.close();
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
        System.out.println("edge load over! index "+count);  
        //_end
    }
    
    public void loadingExEdge(String exPath,int flag) {
		//_start  ��û��ֱ�������Ķ���ľ���
		//HashMap<Integer, HashMap<Integer, Float>> keyEdgeSet=new HashMap<Integer, HashMap<Integer,Float>>();//Ȩ�ر����ݼ�
	    int exCount=0;
        File exFile = new File(exPath);
        if (exFile.isDirectory()) {
            String[] filelist = exFile.list();
            for (int i = 0; i < filelist.length; i++) {
            	
            	File readfile;
            	if (flag==0) {
            		readfile = new File(exPath + "/" + filelist[i]);
				}else {
					readfile = new File(exPath + "\\" + filelist[i]);
				}
            	
                //File readfile = new File(exPath + "\\" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(new FileInputStream(readfile), "UTF-8"));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                        	String[] tmpStr=line.split(",");
                        	int x=Integer.parseInt(tmpStr[0]);
                        	int y=Integer.parseInt(tmpStr[1]);
                        	float weight=Float.parseFloat(tmpStr[2]);
                        	
                        	if (!_exKeyEdgeSet.containsKey(x)) {
                        		_exKeyEdgeSet.put(x, new HashMap<Integer, Float>());
							}
                        	_exKeyEdgeSet.get(x).put(y, weight);
                        	exCount++;
                        	if (exCount%25000==0) {
								System.out.println("loading edge index   "+exCount);
							}
                        }
						reader.close();
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
        System.out.println("exEdge load over! index "+exCount);  
        //_end
	}
    //初始化网络
	public void initial(String path,String exPath,int flag) throws InterruptedException {//�Դ�Ȩ�صıߣ���ʼ�������
		loadingEdge(path,flag);//加载社区
		loadingExEdge(exPath,flag);//加载非直接相连的点
        //_start  ��ʼ���ߵľ��룬����û�����ӵıߵľ��루�������̰߳汾��
        /*
        InitialEdge iEdgeSet=new InitialEdge();
        int edgeCount=0;
        Iterator iter = keyEdgeSet.entrySet().iterator();
        //int thrNum=1;
        int thrEdgeCount=0;
        HashSet<String> exRecord=new HashSet<String>();
        //ExecutorService exe = Executors.newFixedThreadPool(thrNum+2);
        exe = Executors.newFixedThreadPool(thrNum+2);
        while (iter.hasNext()) {
        	
            Map.Entry<Integer,HashMap<Integer, Float>> entry = (Map.Entry<Integer,HashMap<Integer, Float>>) iter.next();
            int x=entry.getKey();
            HashMap<Integer, Float> val = entry.getValue();
            if (val!=null){
            	Iterator iter2 = val.entrySet().iterator();
            	while (iter2.hasNext()) {
            		if (thrEdgeCount>=thrNum) {
            			exe.shutdown();
                        while (true) {
                            if (exe.isTerminated()) {
                                System.out.println("�߳�   "+(edgeCount-1)+" �����ˣ�");
                                break;
                            }
                            Thread.sleep(200);
                        }
                        thrEdgeCount=0;
                        exe=null;
                        exe = Executors.newFixedThreadPool(thrNum+2);
					}
            		Map.Entry<Integer, Float> entry2 = (Map.Entry<Integer, Float>) iter2.next();
					int y=entry2.getKey();
					float weight=entry2.getValue();
					if (thrEdgeCount<thrNum) {
						MulCTDD md=new MulCTDD();
						//InitialThread tmpThr=md.new InitialThread("edgeThr "+edgeCount, iEdgeSet, neigbhor, 0, x, y, weight);
						//exe.execute(tmpThr);
						thrEdgeCount++;
						edgeCount++;
					}
					thrEdgeCount=calDistanceForEN(x, y, edgeCount, thrEdgeCount, exRecord, iEdgeSet);
					if (edgeCount%5==0) {
						System.out.println("********************************");
						System.out.println("********************************");
						System.out.println("********************************");
						System.out.println("********************************");
						System.out.println("initial edge index   "+edgeCount);
						System.out.println("********************************");
						System.out.println("********************************");
						System.out.println("********************************");
						System.out.println("********************************");
					}
				}
            }
            
        }
        exe.shutdown();
        while (true) {
            if (exe.isTerminated()) {
                System.out.println("�߳̽����ˣ�");
                break;
            }
            Thread.sleep(200);
        }
        
        System.out.println("initial edge over!  index with "+edgeCount);
       

        _keyEdgeSet=iEdgeSet.getEdge();
        _exKeyEdgeSet=iEdgeSet.getExEdge();
        */
        //_end
	}
	
	public float DI(int x,int y,float dist) {
    	HashMap<Integer, Float> xNeighbor=getNei(x);
    	HashMap<Integer, Float> yNeighbor=getNei(y);
    	
    	double _similar=(double)(1-dist);
    	return (float)(-(Math.sin(_similar)/xNeighbor.size()+Math.sin(_similar)/yNeighbor.size()));
	}
	
	public double getDistance(int x, int y) {
		if (_keyEdgeSet.containsKey(x)) {
			if (_keyEdgeSet.get(x).containsKey(y)) {
				return(double)_keyEdgeSet.get(x).get(y);
			}
		}
		return (double)_keyEdgeSet.get(y).get(x);
	}
	
	public void setDistance(int x, int y,float dist) {
		boolean flag=false;
		if (_keyEdgeSet.containsKey(x)) {
			if (_keyEdgeSet.get(x).containsKey(y)) {
				_keyEdgeSet.get(x).put(y, dist);
				flag=true;
			}
		}
		if (!flag) {
			_keyEdgeSet.get(y).put(x, dist);
		}
	}
	
	public double getEXDistance(int x,int y) {
		if (_exKeyEdgeSet.containsKey(x)) {
			if (_exKeyEdgeSet.get(x).containsKey(y)) {
				return (double)_exKeyEdgeSet.get(x).get(y);
			}
		}
		return (double)_exKeyEdgeSet.get(y).get(x);
		
	}
	
	public float CI(int x,int y) {
    	HashMap<Integer, Float> xNeighbor=getNei(x);
    	HashMap<Integer, Float> yNeighbor=getNei(y);
    	
    	float retValue=0;
        Iterator iter = xNeighbor.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
            int key=entry.getKey();
            if (yNeighbor.containsKey(key)) {
            	double tmpCi=0;
            	tmpCi+=Math.sin(1-getDistance(key, x))*(1-getDistance(key, y))/xNeighbor.size();
            	tmpCi+=Math.sin(1-getDistance(key, y))*(1-getDistance(key, x))/yNeighbor.size();
            	retValue+=(float)-tmpCi;
			}
        }
       return retValue; 
	}
	
	public double calEXNeighbor(int x,int y,double lmta) {
		double retValue=1-getEXDistance(x, y);
		if (retValue<lmta) {
			retValue=retValue-lmta;
		}
		return retValue;
	}
	
	public float EI(int x,int y,float lmta) {
		
    	HashMap<Integer, Float> xNeighbor=getNei(x);
    	HashMap<Integer, Float> yNeighbor=getNei(y);
    	
    	HashSet<Integer> xEN=getEN(x, y);
    	HashSet<Integer> yEN=getEN(y, x);
    	
    	double retVaule=0;
    	for (Integer xNode : xEN) {
    		retVaule+=Math.sin(1-getDistance(xNode, x))*calEXNeighbor(xNode, y, lmta)/xNeighbor.size();
		}
    	
    	for (Integer yNode : yEN) {
    		retVaule+=Math.sin(1-getDistance(yNode, y))*calEXNeighbor(yNode, x, lmta)/yNeighbor.size();
		}
    	
    	return (float)-retVaule;
	}
	
	public void CDRun(String path,float lmta,int maxRep,String exPath,int flag) throws InterruptedException {
		initial(path,exPath,flag);
		boolean stopFlag=true;
		int rep=0;
		while (stopFlag&&(rep<maxRep)) {
			stopFlag=false;
	        Iterator iter = _keyEdgeSet.entrySet().iterator();
	        int edgeCount=0;
	        while (iter.hasNext()) {
	            Map.Entry<Integer,HashMap<Integer, Float>> entry = (Map.Entry<Integer,HashMap<Integer, Float>>) iter.next();
	            int x=entry.getKey();
	            HashMap<Integer, Float> val = entry.getValue();
	            if (val!=null){
	            	Iterator iter2 = val.entrySet().iterator();
	            	while (iter2.hasNext()) {
	            		edgeCount++;
	            		if (edgeCount%50000==0) {
	            			System.out.println("updata edge: repeat "+rep+"   index "+edgeCount+"      Time: "+new Date());
						}
	            		Map.Entry<Integer, Float> entry2 = (Map.Entry<Integer, Float>) iter2.next();
						int y=entry2.getKey();
						float dist=entry2.getValue();
						if (dist>0&&dist<1) {
							float thta=(float)(DI(x, y, dist)+CI(x, y)+EI(x, y, lmta));
							if (thta!=0) {
								dist=dist+thta;
								if (dist>1) {
									dist=1;
								}
								if (dist<0) {
									dist=0;
								}
								if (dist>0&&dist<1) {
									stopFlag=true;
								}
								setDistance(x, y, dist);
							}
						}
					}
	            }
	        }
	        rep++;
		}
	}
	
	public void printEdge(String path,int flag) {
        Iterator iter = _keyEdgeSet.entrySet().iterator();
        int edgeCount=0;
        String saveString="";
        while (iter.hasNext()) {
            Map.Entry<Integer,HashMap<Integer, Float>> entry = (Map.Entry<Integer,HashMap<Integer, Float>>) iter.next();
            int x=entry.getKey();
            HashMap<Integer, Float> val = entry.getValue();
            if (val!=null){
            	Iterator iter2 = val.entrySet().iterator();
            	while (iter2.hasNext()) {
            		Map.Entry<Integer, Float> entry2 = (Map.Entry<Integer, Float>) iter2.next();
					int y=entry2.getKey();
					float dist=entry2.getValue();
					if (dist<1) {
						saveString+="("+x+","+y+","+dist+")\n";
	            		edgeCount++;
	            		
	            		if (edgeCount%100000==0) {
	            			System.out.println("save edge  index "+edgeCount);
	            			if (flag==0) {
	            				WriteTxt(saveString, path+"/"+edgeCount+".txt");
							}else {
								WriteTxt(saveString, path+"\\"+edgeCount+".txt");
							}
	            			saveString="";
						}
					}
				}
            }
        }
        if (saveString!="") {
			if (flag==0) {
				WriteTxt(saveString, path+"/"+edgeCount+".txt");
			}else {
				WriteTxt(saveString, path+"\\"+edgeCount+".txt");
			}
		}
        System.out.println("save edge  over !");
	}
	
    public static void WriteTxt(String Text,String Spath){
        if (Spath.equals(""))
        {
            try {
                FileWriter writer = new FileWriter("./default/Result.txt", true);
                writer.write(Text);
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            try {
                File file=new File(Spath);
                if (!file.exists()){
                    if (!file.getParentFile().exists()){
                        file.getParentFile().mkdirs();
                    }
                    file.createNewFile();
                }
                FileWriter writer = new FileWriter(Spath, true);
                writer.write(Text);
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		if (args.length>1) {
			// TODO Auto-generated method stub
			int flag=Integer.parseInt(args[3]);// 0 linux 1 win
			float lmta=Float.parseFloat(args[4]);//系数
			int maxRepet=Integer.parseInt(args[5]);//最大重复次数
			MulCTDD ctdd=new MulCTDD();
			//ctdd.CDRun("G:\\communityDetection\\test\\result\\edge", 0.78f, 20,"G:\\communityDetection\\test\\result\\exEdge");
			ctdd.CDRun(args[0], lmta, maxRepet,args[1],flag);
			//ctdd.printEdge("G:\\communityDetection\\communityResult\\test");
			ctdd.printEdge(args[2],flag);
		}else {
			// TODO Auto-generated method stub
			MulCTDD ctdd=new MulCTDD();
			//ctdd.CDRun("G:\\communityDetection\\test\\result\\edge", 0.78f, 20,"G:\\communityDetection\\test\\result\\exEdge");
			ctdd.CDRun("G:\\communityDetection\\initialNet\\edge", 0.7f, 13,"G:\\communityDetection\\initialNet\\exEdge",1);
			//ctdd.printEdge("G:\\communityDetection\\communityResult\\test");
			ctdd.printEdge("G:\\communityDetection\\communityResult\\port2015By01",1);
		}
	}
	
	//_start
	/*
	 * 
	class InitialThread extends Thread {

		private InitialEdge iEdgeSet;
		private HashMap<Integer,HashMap<Integer, Float>> neigbhor;
	    int flag;
	    int x;
	    int y;
	    float value;
	    
	    public InitialThread(String name,InitialEdge iEdgeSet,
	    		HashMap<Integer,HashMap<Integer, Float>> neigbhor,
	    		int flag,int x,int y,float value){
	    	super(name);
	    	this.iEdgeSet=iEdgeSet;
	    	this.neigbhor=neigbhor;
	    	this.flag=flag;
	    	this.x=x;
	    	this.y=y;
	    	this.value=value;
	    	
	    }
	    
	    public HashMap<Integer, Float> getNei(int x) {
	    	HashMap<Integer, Float> _neighbor=new HashMap<Integer, Float>();
	    	HashMap<Integer, Float> nei=neigbhor.get(x);
	    	if (nei!=null) {
	    		_neighbor.putAll(nei);
			}
	    	return _neighbor;
		}
	    
	    public HashSet<Integer> getNode(HashMap<Integer, Float> data) {
			HashSet<Integer> retValue=new HashSet<Integer>();
	        Iterator iter = data.entrySet().iterator();
	        while (iter.hasNext()) {
	            Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
	            int key=entry.getKey();
	            retValue.add(key);
	        }
	        return retValue;
		}
	    
	    public void run() {
	    	System.out.println(this.getName());
	    	HashMap<Integer, Float> xNeighbor=getNei(x);
	    	HashMap<Integer, Float> yNeighbor=getNei(y);
	    	
	    	HashSet<Integer> _unionNode=new HashSet<Integer>();
	    	float _interact=0;
	    	float _union=0;
	        Iterator iter = xNeighbor.entrySet().iterator();
	        while (iter.hasNext()) {
	            Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
	            int key=entry.getKey();
	            float val=entry.getValue();
	            if (yNeighbor.containsKey(key)) {
					_interact+=val;
					_interact+=yNeighbor.get(key);
				}
	            _unionNode.add(key);
	        }
	        xNeighbor=null;
	    	_interact+=value;
	    	
	    	_unionNode.addAll(getNode(yNeighbor));
	    	if (flag!=0) {
				_unionNode.add(this.x);
				_unionNode.add(this.y);
			}
	    	
	    	yNeighbor=null;
	    	HashSet<String> record=new HashSet<String>();
	    	for (Integer node : _unionNode) {
	    		HashMap<Integer, Float> tmpNeighbor=getNei(node);
	            Iterator tmpIter = tmpNeighbor.entrySet().iterator();
	            while (tmpIter.hasNext()) {
	                Map.Entry<Integer,Float> tmpentry = (Map.Entry<Integer,Float>) tmpIter.next();
	                int key=tmpentry.getKey();
	                if (_unionNode.contains(key)) {
						String tmpRecord=node+"-"+key;
						String tmpRecord2=key+"-"+node;
						if (!record.contains(tmpRecord)) {
							_union+=tmpentry.getValue();
							record.add(tmpRecord);
							record.add(tmpRecord2);
						}
					}
	            }
	            tmpNeighbor=null;
			}
	    	record=null;
	    	float dist;
	    	if (_union==0) {
	    		dist=1;
			}else {
				dist=1-(_interact/_union);
			}
			
	    	if (flag==0) {
				synchronized (iEdgeSet) {
					iEdgeSet.setEdge(x, y, dist);
				}
			}else {
				synchronized (iEdgeSet) {
					iEdgeSet.setExEdge(x, y, dist);
				}
			}
		}
	}
	
    */
	//_end
	
}
