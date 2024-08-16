/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clustering;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.gephi.appearance.api.AppearanceController;
import org.gephi.appearance.api.AppearanceModel;
import org.gephi.graph.api.Graph;
import org.gephi.graph.api.GraphController;
import org.gephi.graph.api.GraphModel;
import org.gephi.io.importer.api.Container;
import org.gephi.io.importer.api.ImportController;
import org.gephi.io.processor.plugin.DefaultProcessor;
import org.gephi.project.api.ProjectController;
import org.gephi.project.api.Workspace;
import org.openide.util.Lookup;

/**
 *
 * @author hyy
 */
public class Clustering_2v {
    
        //txt转换成csv
    public static void convertFile(String input, String output){
        HashMap<Integer,Integer> nodeMap=getNodesMap(input);
        //HashSet<String> nodeSet=new HashSet<String>();
        String wirteString=getNodeStr(nodeMap);
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(input),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmpStrings=line.split(" ");
                int sou=Integer.parseInt(tmpStrings[0]);
                int tar=Integer.parseInt(tmpStrings[1]);
                //wirteString+=tmpStrings[0].trim()+","+tmpStrings[1].trim()+","+Math.random()+"\n";
                wirteString+="  edge\n  [\n    source "+nodeMap.get(sou)+"\n    target "+nodeMap.get(tar)+"\n    value "+Math.random()+"\n  ]\n";
                //wirteString+=nodeMap.get(sou)+","+nodeMap.get(tar)+","+Math.random()+"\n";
                //nodeSet.add(tmpStrings[0]);
                //nodeSet.add(tmpStrings[1]);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        wirteString+="]";
        FileWrite.WriteTxt(wirteString, output);
        //System.out.println("Node number: "+nodeSet.size());
    }
    
    public static String getNodeStr(HashMap<Integer,Integer> nodeMap){
        String retString="Convert graph by hyy\ngraph\n[\n";
        for (Map.Entry<Integer, Integer> entry : nodeMap.entrySet()) {
            Integer key = entry.getKey();
            Integer value = entry.getValue();
            retString+="  node\n  [\n    id "+value+"\n    label \""+key+"\"\n  ]\n";
        }
        return retString;
    }
    
    public static HashMap<Integer,Integer> getNodesMap(String input){
        HashSet<Integer> nodeSet=new HashSet<Integer>();
        //String wirteString="";
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(input),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmpStrings=line.split(" ");
                //wirteString+=tmpStrings[0].trim()+","+tmpStrings[1].trim()+","+Math.random()+"\n";
                //wirteString+=tmpStrings[0]+","+tmpStrings[1]+","+Math.random()+"\n";
                nodeSet.add(Integer.parseInt(tmpStrings[0]));
                nodeSet.add(Integer.parseInt(tmpStrings[1]));
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        //FileWrite.WriteTxt(wirteString, output);
        System.out.println("Node number: "+nodeSet.size());
        HashMap<Integer,Integer> nodeMap=new HashMap<Integer,Integer>();
        int index=0;
        for (Integer integer : nodeSet) {
            nodeMap.put(integer, index);
            index++;
        }
        return nodeMap;
    }
    
    public static HashMap<Integer,Integer> getNodesMap_Mul(String input){
        HashSet<Integer> nodeSet=new HashSet<Integer>();
        
        //String wirteString="";
        //int index=0;
        File file = new File(input);
        if (!file.isDirectory()) {
            System.out.println("文件");
            System.out.println("path=" + file.getPath());
            System.out.println("absolutepath=" + file.getAbsolutePath());
            System.out.println("name=" + file.getName());
        } else if (file.isDirectory()) {
            System.out.println("文件夹");
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readfile = new File(input + "/" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader=null;
                    try {
                        reader=new BufferedReader(new InputStreamReader(new FileInputStream(readfile),"UTF-8"));
                        String line=null;
                        while ((line=reader.readLine())!=null){
                            //String[] tmpStrings=line.split(" ");
                            //wirteString+=line.substring(1,line.length()-1)+"\n";
                            String[] tmpStr=line.substring(1,line.length()-1).split(",",2);
                            Integer dataID=Integer.parseInt(tmpStr[0]);
                            nodeSet.add(dataID);
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e1) {
                            }
                        }
                    }
                }
            }
        }
        //String wirteString="";
        System.out.println("Node number: "+nodeSet.size());
        HashMap<Integer,Integer> nodeMap=new HashMap<Integer,Integer>();
        int index=0;
        for (Integer integer : nodeSet) {
            nodeMap.put(integer, index);
            index++;
        }
        return nodeMap;
    }
    
    public static void convertMulFile(String input, String output,String nodeInput){
        HashSet<Integer> nodeSet=new HashSet<Integer>();
        HashMap<Integer,Integer> nodeMap=getNodesMap_Mul(nodeInput);
        //HashSet<String> nodeSet=new HashSet<String>();
        String wirteString=getNodeStr(nodeMap);
        FileWrite.WriteTxt(wirteString, output);
        wirteString="";
        //String wirteString="";
        int index=0;
        File file = new File(input);
        if (!file.isDirectory()) {
            System.out.println("文件");
            System.out.println("path=" + file.getPath());
            System.out.println("absolutepath=" + file.getAbsolutePath());
            System.out.println("name=" + file.getName());
        } else if (file.isDirectory()) {
            System.out.println("文件夹");
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readfile = new File(input + "/" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader=null;
                    try {
                        reader=new BufferedReader(new InputStreamReader(new FileInputStream(readfile),"UTF-8"));
                        String line=null;
                        while ((line=reader.readLine())!=null){
                            String[] tmpStrings=line.substring(1,line.length()-1).split(",");
                            int sou=Integer.parseInt(tmpStrings[0]);
                            int tar=Integer.parseInt(tmpStrings[1]);
                            //wirteString+=tmpStrings[0].trim()+","+tmpStrings[1].trim()+","+Math.random()+"\n";
                            wirteString+="  edge\n  [\n    source "+nodeMap.get(sou)+"\n    target "+nodeMap.get(tar)+"\n    value "+tmpStrings[2]+"\n  ]\n";
                            nodeSet.add(sou);
                            nodeSet.add(tar);
                            //String[] tmpStrings=line.split(" ");
                            //wirteString+=line.substring(1,line.length()-1)+"\n";
                            index++;
                            if (index%5000==0) {
                                FileWrite.WriteTxt(wirteString, output);
                                wirteString="";
                                System.out.println("Wirte index: "+index);
                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e1) {
                            }
                        }
                    }
                }
            }
        }
        wirteString+="]";
        if (wirteString!="") {
            FileWrite.WriteTxt(wirteString, output);
            System.out.println("Wirte index: "+index);
        }
        System.out.println("The node number with edge :"+nodeSet.size());
        System.out.println("Wirte complete !");
    }

    public static void getCluster(String input,String output,HashMap<Integer,Integer> clusteringID,int clusterSize){
        //System.out.println("Final cluster size: "+clusterSize);
        System.out.println("Cluster size: "+clusterSize);
        String wirteString="";
        int index=0;
        File file = new File(input);
        if (!file.isDirectory()) {
            System.out.println("文件");
            System.out.println("path=" + file.getPath());
            System.out.println("absolutepath=" + file.getAbsolutePath());
            System.out.println("name=" + file.getName());
        } else if (file.isDirectory()) {
            System.out.println("文件夹");
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readfile = new File(input + "/" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader=null;
                    try {
                        reader=new BufferedReader(new InputStreamReader(new FileInputStream(readfile),"UTF-8"));
                        String line=null;
                        while ((line=reader.readLine())!=null){
                            //String[] tmpStrings=line.split(" ");
                            //wirteString+=line.substring(1,line.length()-1)+"\n";
                            String[] tmpStr=line.substring(1,line.length()-1).split(",",2);
                            Integer dataID=Integer.parseInt(tmpStr[0]);
                            if (!clusteringID.containsKey(dataID)) {
                                wirteString+="("+clusterSize+","+tmpStr[1]+")\n";
                                clusterSize++;
                            }else{
                                wirteString+="("+clusteringID.get(dataID)+","+tmpStr[1]+")\n";
                            }
                            index++;
                            if (index%1000==0) {
                                FileWrite.WriteTxt(wirteString, output);
                                wirteString="";
                                //System.out.println("Wirte index: "+index);
                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e1) {
                            }
                        }
                    }
                }
            }
        }
        if (wirteString!="") {
            FileWrite.WriteTxt(wirteString, output);
            System.out.println("Wirte index: "+index);
        }
        System.out.println("Final cluster size: "+clusterSize);
        System.out.println("Wirte complete !");  
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        //System.out.println(" Start !!!");
        //String retString="Convert graph by hyy\ngraph\n[\n";
        //retString+="\tnode\n\t[\n\t\tid "+value+"\n\t\tlabel \""+key+"\"\n\t]\n";
        //System.out.print("Convert graph by hyy\ngraph\n[\n"+"\tnode\n\t[\n\t\tid "+5+"\n\t\tlabel \""+10+"\"\n\t]\n");
        //convertFile("/Users/hyy/PycharmProjects/pylouvain-master/data/arxiv.txt", "/Users/hyy/PycharmProjects/pylouvain-master/data/arxiv1.gml");
        
        String intputPathString=args[0];
        //String outputPathString=args[1];
        //int cnFlag=Integer.parseInt(args[2]);
        String nodePathString=args[1];
        
        String clusteringSavePath=args[2];
        
        int iterCount=Integer.parseInt(args[3]);
        
        double interval=Double.parseDouble(args[4]);
        
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        System.out.println("Start:  "+df.format(new Date()));// new Date()为获取当前系统时间
        
        //String clusteringSave=args[4];
        //int random=Integer.parseInt(args[5]);
        //double resolution=Double.parseDouble(args[6]);
        //int isClustering=Integer.parseInt(args[7]);
        
        
        //if (cnFlag==1) {
        //    convertFile(intputPathString, outputPathString);
        //    intputPathString=outputPathString;
        //}else if (cnFlag==2) {
        //    convertMulFile(intputPathString, outputPathString,nodePathString);
        //    intputPathString=outputPathString;
        //}
        
        //convertFile("/Users/hyy/PycharmProjects/pylouvain-master/data/arxiv.txt", "/Users/hyy/PycharmProjects/pylouvain-master/data/arxiv.csv");
        
        //Init a project - and therefore a workspace
        //ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
        //pc.newProject();
        //Workspace workspace = pc.getCurrentWorkspace();
        //GraphModel gm=
        
        //Graph
        //Modularity 
        //Get models and controllers for this new workspace - will be useful later
        //AttributeModel attributeModel = Lookup.getDefault().lookup(AttributeController.class).getModel();
        //GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getModel();
        //PreviewModel model = Lookup.getDefault().lookup(PreviewController.class).getModel();
        //ImportController importController = Lookup.getDefault().lookup(ImportController.class);
        //FilterController filterController = Lookup.getDefault().lookup(FilterController.class);
        //RankingController rankingController = Lookup.getDefault().lookup(RankingController.class);
        int isClustering=1;
        if (isClustering==1) {
        //Init a project - and therefore a workspace
        ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
        pc.newProject();
        Workspace workspace = pc.getCurrentWorkspace();

        //GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getGraphModel(workspace);
        //Get controllers and models
        ImportController importController = Lookup.getDefault().lookup(ImportController.class);
        GraphModel graphModel = Lookup.getDefault().lookup(GraphController.class).getGraphModel();
        AppearanceModel appearanceModel = Lookup.getDefault().lookup(AppearanceController.class).getModel();

        //Import file
        Container container;
        try {
            //File file = new File(getClass().getResource("/org/gephi/toolkit/demos/polblogs.gml").toURI());
            //File file = new File("/Users/hyy/PycharmProjects/pylouvain-master/data/lesmis.gml");
            File file = new File(intputPathString);
            container = importController.importFile(file);
        } catch (Exception ex) {
            ex.printStackTrace();
            return;
        }

        //Append imported data to GraphAPI
        importController.process(container, new DefaultProcessor(), workspace);
        
        System.out.println("Graph load complete:  "+df.format(new Date()));// new Date()为获取当前系统时间
        //System.out.println("!!!");
        
        //java -Xmx130000m -jar Clustering.jar edge_3_0.5_1.gml -1 0 norBySum_filter 
        //./result/r_3_075_1_1_0.946/cResult_3_075_1_1_0.946.txt 1 0.946 1

        //java -Xmx220000m -jar Clustering.jar edge_3_0.5_1.gml norBySum_filter ./result_2v 300 0.005
        Graph graph = graphModel.getUndirectedGraphVisible();
        
        System.out.println("Convert to undirectedGraph complete:  "+df.format(new Date()));// new Date()为获取当前系统时间
        
        Modularity_4v stConstructor=new Modularity_4v();
        int nodeNumber=graph.getNodeCount();
        Modularity_4v.CommunityStructure structure=stConstructor.new CommunityStructure(graph);
        
        System.out.println("Convert to undirectedGraph complete:  "+df.format(new Date()));// new Date()为获取当前系统时间
        
        //java -Xmx130000m -jar Clustering.jar edge_3_0.5_1.gml -1 0 norBySum_filter 
        // ./result/r_3_075_1_1_0.702/cResult_3_075_1_1_0.702.txt 1 0.702 1

        
        for (int i = 0; i < iterCount; i++) {
            
            //Modularity_2v mlModularity=new Modularity_2v();
            Modularity_4v mlModularity=new Modularity_4v();
            mlModularity.setRandom(true);
            
            double resolution=1.0-(interval*i);
            mlModularity.setResolution(resolution);
            
            DecimalFormat dfr = new DecimalFormat("#.000");
            String rString=dfr.format(resolution);
            
            String clusteringSave=clusteringSavePath+"/r_3_050_1_1_"+rString+"/cResult_3_050_1_1_"+rString+".txt";
                    
            Modularity_4v.CommunityStructure tmp_structureCommunityStructure=stConstructor.clone_structure(structure, nodeNumber);
            System.out.println("Iter  "+i+"th  clone stucture complete:  "+df.format(new Date()));// new Date()为获取当前系统时间
            
            //mlModularity.execute(graphModel);
            mlModularity.execute_4v(graph, tmp_structureCommunityStructure);
            int commSize=mlModularity.getCommunitySize();
            HashMap<Integer,Integer> comm=mlModularity.getCommunity();
            
            //String clusteringSave=args[4];
            getCluster(nodePathString, clusteringSave, comm, commSize);
            System.out.println("Iter  "+i+"th  clustering complete:  "+df.format(new Date()));// new Date()为获取当前系统时间
            }
        }
        
        
        //String spString=mlModularity.getReport();
        //DirectedGraph graph = graphModel.getDirectedGraph();
        //System.out.println("Nodes: " + graph.getNodeCount());
        //System.out.println("Edges: " + graph.getEdgeCount());
        //System.out.println("RP: " + spString);
        
    }
    
}
