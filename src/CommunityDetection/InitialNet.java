package CommunityDetection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by hdp on 17-8-16.
 */
public class InitialNet {

    //加载所有的边，形成邻居MAP
    public static HashMap<Integer,HashMap<Integer, Float>> loadingEdge(String inputPath) throws IOException {
        HashMap<Integer,HashMap<Integer, Float>> neigbhor=new HashMap<Integer, HashMap<Integer,Float>>();
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++) {
            if (!fileList[i].isDirectory()) {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] tmpStr=line.substring(1,line.length()-1).split(",");
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
                }
            }
        }
        return neigbhor;
    }

    //获得和点x相连的所有边
    public static HashMap<Integer, Float> getNei(int x,HashMap<Integer,HashMap<Integer, Float>> neigbhor) {
        //HashMap<Integer, Float> _neighbor=new HashMap<Integer, Float>();
        HashMap<Integer, Float> nei=neigbhor.get(x);
        //if (nei!=null) {
            //_neighbor.putAll(nei);
        //}
        //return _neighbor;
        return nei;
    }

    //获取边的顶点
    public static HashSet<Integer> getNode(HashMap<Integer, Float> data) {
        HashSet<Integer> retValue=new HashSet<Integer>();
        Iterator iter = data.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
            int key=entry.getKey();
            retValue.add(key);
        }
        return retValue;
    }

    //计算点之间的距离
    public static float calDistance(int x,int y,float value,HashMap<Integer,HashMap<Integer, Float>> neigbhor){
        HashMap<Integer, Float> xNeighbor=getNei(x,neigbhor);
        HashMap<Integer, Float> yNeighbor=getNei(y,neigbhor);

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
        if (!_unionNode.contains(x)){
            _unionNode.add(x);
        }

        if (!_unionNode.contains(y)){
            _unionNode.add(y);
        }
        yNeighbor=null;
        HashSet<String> record=new HashSet<String>();
        for (Integer node : _unionNode) {
            HashMap<Integer, Float> tmpNeighbor=getNei(node,neigbhor);
            Iterator tmpIter = tmpNeighbor.entrySet().iterator();
            while (tmpIter.hasNext()) {
                Map.Entry<Integer,Float> tmpentry = (Map.Entry<Integer,Float>) tmpIter.next();
                int key=tmpentry.getKey();
                if (_unionNode.contains(key)) {
                    String tmpRecord=node+"-"+key;
                    String tmpRecord2=key+"-"+node;
                    if ((!record.contains(tmpRecord))&&(!record.contains(tmpRecord2))) {
                        _union+=tmpentry.getValue();
                        record.add(tmpRecord);
                        //record.add(tmpRecord2);
                    }
                }
            }
            tmpNeighbor=null;
        }
        _unionNode=null;
        record=null;
        float dist;
        if (_union==0) {
            dist=1;
        }else {
            dist=1-(_interact/_union);
        }
        return dist;
    }

    //获取x的邻居，且不属于y的邻居的节点
    public static HashSet<Integer> getEN(int x,int y,HashMap<Integer,HashMap<Integer, Float>> neigbhor) {
        HashMap<Integer, Float> xNeighbor=getNei(x,neigbhor);
        HashMap<Integer, Float> yNeighbor=getNei(y,neigbhor);

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

    //计算没有边之间相连的定点的距离
    public static ArrayList<String> calDistanceForEN(int x,int y,HashMap<Integer,HashMap<Integer, Float>> neigbhor) throws InterruptedException {
        ArrayList<String> retVaule=new ArrayList<String>();
        HashSet<Integer> xEN=getEN(x, y,neigbhor);
        HashSet<Integer> yEN=getEN(y, x,neigbhor);
        for (Integer yNode : yEN) {
            float tmpDist=calDistance(x,yNode,0,neigbhor);
            String tmpStr;
            if (x>yNode){
                tmpStr=yNode+","+x+","+tmpDist+","+1;
            }else {
                tmpStr=x+","+yNode+","+tmpDist+","+1;
            }
            retVaule.add(tmpStr);
        }

        for (Integer xNode : xEN) {
            float tmpDist=calDistance(y,xNode,0,neigbhor);
            String tmpStr;
            if (y>xNode){
                tmpStr=xNode+","+y+","+tmpDist+","+1;
            }else {
                tmpStr=y+","+xNode+","+tmpDist+","+1;
            }
            retVaule.add(tmpStr);
        }

        return retVaule;
    }


    public static ArrayList<String> getENEdge(int x,int y,HashMap<Integer,HashMap<Integer, Float>> neigbhor) throws InterruptedException {
        ArrayList<String> retVaule=new ArrayList<String>();
        HashSet<Integer> xEN=getEN(x, y,neigbhor);
        HashSet<Integer> yEN=getEN(y, x,neigbhor);
        for (Integer yNode : yEN) {
            if (x!=yNode){
                String tmpStr;
                if (x>yNode){
                    tmpStr=yNode+","+x+","+"-1";
                }else {
                    tmpStr=x+","+yNode+","+"-1";
                }
                retVaule.add(tmpStr);
            }
        }

        for (Integer xNode : xEN) {
            if (y!=xNode){
                String tmpStr;
                if (y>xNode){
                    tmpStr=xNode+","+y+","+"-1";
                }else {
                    tmpStr=y+","+xNode+","+"-1";
                }
                retVaule.add(tmpStr);
            }
        }

        return retVaule;
    }

    //region  注释
    /*
    public static void initial(JavaRDD<String> edgeSet, String inputPath, JavaSparkContext sc,String savePath) throws IOException {
        HashMap<Integer,HashMap<Integer, Float>> neigbhor=loadingEdge(inputPath);
        final Broadcast<HashMap<Integer,HashMap<Integer, Float>>> brNeigbhor=sc.broadcast(neigbhor);

        class calEdgeDistance implements FlatMapFunction<String,String>{
            @Override
            public Iterable<String> call(String s) throws Exception {

                List<String> retVaule=new ArrayList<String>();

                HashMap<Integer,HashMap<Integer, Float>> _neigbhor=brNeigbhor.getValue();
                String[] tmpStr=s.substring(1,s.length()-1).split(",");
                int x=Integer.parseInt(tmpStr[0]);
                int y=Integer.parseInt(tmpStr[1]);
                float weight=Float.parseFloat(tmpStr[2]);
                float xyDist=calDistance(x,y,weight,_neigbhor);
                retVaule.add(x+","+y+","+xyDist+","+0);

                ArrayList<String> enSet=calDistanceForEN(x,y,_neigbhor);
                if (enSet.size()>0){
                    retVaule.addAll(enSet);
                }
                return retVaule;
            }
        }

        class flitEdeg implements Function<String,Boolean>{
            @Override
            public Boolean call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                if (tmpStr[3]=="0"){
                    return true;
                }else {
                    return false;
                }
            }
        }

        class flitExEdeg implements Function<String,Boolean>{
            @Override
            public Boolean call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                if (tmpStr[3]=="1"){
                    return true;
                }else {
                    return false;
                }
            }
        }

        class transKeyForReduce implements PairFunction<String,String,String>{
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                String tmpKey=tmpStr[0]+"-"+tmpStr[1];
                String tmpValue=tmpStr[0]+","+tmpStr[1]+","+tmpStr[2];
                return new Tuple2<String,String>(tmpKey,tmpValue);
            }
        }

        class reduceForEdge implements Function2<String,String,String>{
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }

        class getData implements Function<Tuple2<String,String>,String>{
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2;
            }
        }

        JavaRDD<String> distanceSet=edgeSet.flatMap(new calEdgeDistance()).distinct();
        distanceSet.cache();
        distanceSet.filter(new flitEdeg()).mapToPair(new transKeyForReduce()).
                reduceByKey(new reduceForEdge()).map(new getData()).saveAsTextFile(savePath+"/edge");

        distanceSet.filter(new flitExEdeg()).mapToPair(new transKeyForReduce()).
                reduceByKey(new reduceForEdge()).map(new getData()).saveAsTextFile(savePath+"/exEdge");

    }
    */
    //endregion

    public static void initial2V(JavaRDD<String> edgeSet, JavaSparkContext sc,String savePath,HashMap<Integer,HashMap<Integer, Float>> neigbhor) throws IOException {
        final Broadcast<HashMap<Integer,HashMap<Integer, Float>>> brNeigbhor=sc.broadcast(neigbhor);

        class getAllEdge implements FlatMapFunction<String,String>{
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<String> retVaule=new ArrayList<String>();
                HashMap<Integer,HashMap<Integer, Float>> _neigbhor=brNeigbhor.getValue();
                String[] tmpStr=s.substring(1,s.length()-1).split(",");
                int x=Integer.parseInt(tmpStr[0]);
                int y=Integer.parseInt(tmpStr[1]);
                float weight=Float.parseFloat(tmpStr[2]);
                retVaule.add(x+","+y+","+weight);
                ArrayList<String> enEdge=getENEdge(x,y,_neigbhor);
                if (enEdge!=null){
                    retVaule.addAll(enEdge);
                }
                return retVaule;
            }
        }

        class getReduceKey implements PairFunction<String,String,String>{
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                return new Tuple2<String, String>(tmpStr[0]+"-"+tmpStr[1],s);
            }
        }

        class reduceForEdge implements Function2<String,String,String>{
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }

        class getEdgeData implements Function<Tuple2<String,String>,String>{
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2;
            }
        }

        class calEdgeDist implements Function<String,String>{
            @Override
            public String call(String s) throws Exception {
                HashMap<Integer,HashMap<Integer, Float>> _neigbhor=brNeigbhor.getValue();
                String[] tmpStr=s.split(",");
                int x=Integer.parseInt(tmpStr[0]);
                int y=Integer.parseInt(tmpStr[1]);
                float weight=Float.parseFloat(tmpStr[2]);
                String retVaule="";
                if (weight<0){
                    float xyDist=calDistance(x,y,0,_neigbhor);
                    retVaule=1+","+x+","+y+","+xyDist;
                }else {
                    float xyDist=calDistance(x,y,weight,_neigbhor);
                    retVaule=0+","+x+","+y+","+xyDist;
                }
                return retVaule;
            }
        }

        class flitEdeg implements Function<String,Boolean>{
            @Override
            public Boolean call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                double flag=Double.parseDouble(tmpStr[0]);
                if (flag<0.5){
                    return true;
                }else {
                    return false;
                }
            }
        }

        class flitExEdeg implements Function<String,Boolean>{
            @Override
            public Boolean call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                double flag=Double.parseDouble(tmpStr[0]);
                if (flag>0.5){
                    return true;
                }else {
                    return false;
                }
            }
        }

        class getData implements Function<String,String>{
            @Override
            public String call(String s) throws Exception {
                return s.split(",",2)[1];
            }
        }


        JavaRDD<String> distanceSet=edgeSet.flatMap(new getAllEdge()).mapToPair(new getReduceKey()).
                reduceByKey(new reduceForEdge()).map(new getEdgeData()).map(new calEdgeDist());
        distanceSet.cache();
        distanceSet.filter(new flitEdeg()).map(new getData()).saveAsTextFile(savePath+"/edge");
        distanceSet.filter(new flitExEdeg()).map(new getData()).saveAsTextFile(savePath+"/exEdge");
        brNeigbhor.unpersist();

    }

    public static void initial_getDistEdge(JavaRDD<String> edgeSet, JavaSparkContext sc,String savePath,HashMap<Integer,HashMap<Integer, Float>> neigbhor) throws IOException {
        final Broadcast<HashMap<Integer,HashMap<Integer, Float>>> brNeigbhor=sc.broadcast(neigbhor);

        class getAllEdge implements FlatMapFunction<String,String>{
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<String> retVaule=new ArrayList<String>();
                HashMap<Integer,HashMap<Integer, Float>> _neigbhor=brNeigbhor.getValue();
                String[] tmpStr=s.substring(1,s.length()-1).split(",");
                int x=Integer.parseInt(tmpStr[0]);
                int y=Integer.parseInt(tmpStr[1]);
                //float weight=Float.parseFloat(tmpStr[2]);
                //retVaule.add(x+","+y+","+weight);
                ArrayList<String> enEdge=getENEdge(x,y,_neigbhor);
                if (enEdge!=null){
                    retVaule.addAll(enEdge);
                }
                return retVaule;
            }
        }

        class getReduceKey implements PairFunction<String,String,String>{
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                return new Tuple2<String, String>(tmpStr[0]+"-"+tmpStr[1],s);
            }
        }

        class reduceForEdge implements Function2<String,String,String>{
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }

        class getEdgeData implements Function<Tuple2<String,String>,String>{
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2;
            }
        }


        JavaRDD<String> distanceSet=edgeSet.flatMap(new getAllEdge()).mapToPair(new getReduceKey()).
                reduceByKey(new reduceForEdge()).map(new getEdgeData());
        distanceSet.saveAsTextFile(savePath+"/tmpExEdge");
        brNeigbhor.unpersist();
    }

    public static void initial3V(JavaRDD<String> edgeSet,JavaSparkContext sc,String savePath,
                                 HashMap<Integer,HashMap<Integer, Float>> neigbhor,int partNum_2) throws IOException {

        final Broadcast<HashMap<Integer,HashMap<Integer, Float>>> brNeigbhor=sc.broadcast(neigbhor);
        class trimForEdge implements Function<String,String>{
            @Override
            public String call(String s) throws Exception {
                return s.substring(1,s.length()-1);
            }
        }
        class calEdgeDist implements Function<String,String>{
            @Override
            public String call(String s) throws Exception {
                HashMap<Integer,HashMap<Integer, Float>> _neigbhor=brNeigbhor.getValue();
                String[] tmpStr=s.split(",");
                int x=Integer.parseInt(tmpStr[0]);
                int y=Integer.parseInt(tmpStr[1]);
                float weight=Float.parseFloat(tmpStr[2]);
                String retVaule="";
                if (weight<0){
                    float xyDist=calDistance(x,y,0,_neigbhor);
                    retVaule=x+","+y+","+xyDist;
                }else {
                    float xyDist=calDistance(x,y,weight,_neigbhor);
                    retVaule=x+","+y+","+xyDist;
                }
                return retVaule;
            }
        }

        edgeSet.map(new trimForEdge()).map(new calEdgeDist()).saveAsTextFile(savePath+"/edge");
        sc.textFile(savePath+"/tmpExEdge").repartition(partNum_2).map(new calEdgeDist()).saveAsTextFile(savePath+"/exEdge");
    }

    public static void main(String[] args) throws IOException {

        String inputPath=args[0];
        String outputPath=args[1];
        int partNum=Integer.parseInt(args[2]);
        int partNum_2=Integer.parseInt(args[3]);
        int flag=Integer.parseInt(args[4]);

        SparkConf conf=new SparkConf().setAppName("InitialNet");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);

        HashMap<Integer,HashMap<Integer, Float>> neigbhor=loadingEdge(inputPath);

        JavaRDD<String> lines = sc.textFile(inputPath).repartition(partNum);
        //initial2V(lines,sc,outputPath,neigbhor);
        if (flag==0){
            initial_getDistEdge(lines,sc,outputPath,neigbhor);
        }
        initial3V(lines,sc,outputPath,neigbhor,partNum_2);
    }
}
