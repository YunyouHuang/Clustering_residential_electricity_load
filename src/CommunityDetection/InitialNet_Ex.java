package CommunityDetection;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
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
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * Created by hyy on 2018/3/12.
 */
public class InitialNet_Ex {

    //加载所有的边，形成邻居MAP
    public static Int2ObjectOpenHashMap loadingEdge(String inputPath) throws IOException {
        //HashMap<Integer,HashMap<Integer, Float>> neigbhor=new HashMap<Integer, HashMap<Integer,Float>>();
        Int2ObjectOpenHashMap neigbhor=new Int2ObjectOpenHashMap();
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
                        neigbhor.put(x, new Int2FloatOpenHashMap());
                    }
                    ((Int2FloatOpenHashMap)(neigbhor.get(x))).put(y, weight);
                    if (!neigbhor.containsKey(y)) {
                        neigbhor.put(y, new Int2FloatOpenHashMap());
                    }
                    ((Int2FloatOpenHashMap)(neigbhor.get(y))).put(x, weight);
                }
            }
        }
        return neigbhor;
    }

    //获得和点x相连的所有边
    public static Int2FloatOpenHashMap getNei(int x,Int2ObjectOpenHashMap neigbhor) {
        //HashMap<Integer, Float> _neighbor=new HashMap<Integer, Float>();
        Int2FloatOpenHashMap nei=(Int2FloatOpenHashMap)(neigbhor.get(x));
        //if (nei!=null) {
        //_neighbor.putAll(nei);
        //}
        //return _neighbor;
        return nei;
    }

    //获取边的顶点
    public static IntOpenHashSet getNode(Int2FloatOpenHashMap data) {
        IntOpenHashSet retValue=new IntOpenHashSet();
        //Iterator iter = data.entrySet().iterator();
        IntIterator iter=data.keySet().iterator();
        while (iter.hasNext()) {
            //Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
            int key=iter.nextInt();
            retValue.add(key);
        }
        return retValue;
    }

    //计算点之间的距离
    public static float calDistance(int x,int y,float value,Int2ObjectOpenHashMap neigbhor){
        Int2FloatOpenHashMap xNeighbor=getNei(x,neigbhor);
        Int2FloatOpenHashMap yNeighbor=getNei(y,neigbhor);

        IntOpenHashSet _unionNode=new IntOpenHashSet();
        float _interact=0;
        float _union=0;
        IntIterator iter_key = xNeighbor.keySet().iterator();
        while (iter_key.hasNext()) {
            //Map.Entry<Integer,Float> entry = (Map.Entry<Integer,Float>) iter.next();
            int key=iter_key.nextInt();
            float val=xNeighbor.get(key);
            if (yNeighbor.containsKey(key)) {
                _interact+=val;
                _interact+=yNeighbor.get(key);
            }
            _unionNode.add(key);
        }
        xNeighbor=null;
        _interact+=value;

        _unionNode.addAll(yNeighbor.keySet());
        if (!_unionNode.contains(x)){
            _unionNode.add(x);
        }

        if (!_unionNode.contains(y)){
            _unionNode.add(y);
        }
        yNeighbor=null;

        ObjectOpenHashSet record=new ObjectOpenHashSet();
        for (int node : _unionNode) {
            Int2FloatOpenHashMap tmpNeighbor=getNei(node,neigbhor);
            //Iterator tmpIter = tmpNeighbor.entrySet().iterator();
            IntIterator tmpIter_key=tmpNeighbor.keySet().iterator();
            while (tmpIter_key.hasNext()) {
                //Map.Entry<Integer,Float> tmpentry = (Map.Entry<Integer,Float>) tmpIter.next();
                int key=tmpIter_key.nextInt();
                if (_unionNode.contains(key)) {
                    String tmpRecord=node+"-"+key;
                    String tmpRecord2=key+"-"+node;
                    if ((!record.contains(tmpRecord))&&(!record.contains(tmpRecord2))) {
                        _union+=tmpNeighbor.get(key);
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
    public static IntOpenHashSet getEN(int x,int y,Int2ObjectOpenHashMap neigbhor) {
        Int2FloatOpenHashMap xNeighbor=getNei(x,neigbhor);
        Int2FloatOpenHashMap yNeighbor=getNei(y,neigbhor);

        IntOpenHashSet xNeighborNodeSet=getNode(xNeighbor);
        //IntSet xNeighborNodeSet=xNeighbor.keySet();
        IntOpenHashSet yNeighborNodeSet=getNode(yNeighbor);
        //IntSet yNeighborNodeSet=yNeighbor.keySet();

        xNeighborNodeSet.removeAll(yNeighborNodeSet);
        if (xNeighborNodeSet.contains(x)) {
            xNeighborNodeSet.remove(x);
        }

        if (xNeighborNodeSet.contains(y)) {
            xNeighborNodeSet.remove(y);
        }
        return xNeighborNodeSet;
    }

    public static ArrayList<String> getENEdge(int x, int y, Int2ObjectOpenHashMap neigbhor) throws InterruptedException {
        ArrayList<String> retVaule=new ArrayList<String>();
        IntOpenHashSet xEN=getEN(x, y,neigbhor);
        IntOpenHashSet yEN=getEN(y, x,neigbhor);

        for (int yNode : yEN) {
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

        for (int xNode : xEN) {
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

    public static void initial_getDistEdge(JavaRDD<String> edgeSet, JavaSparkContext sc, String savePath, Int2ObjectOpenHashMap neigbhor) throws IOException {
        final Broadcast<Int2ObjectOpenHashMap> brNeigbhor=sc.broadcast(neigbhor);

        class getAllEdge implements FlatMapFunction<String,String> {
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<String> retVaule=new ArrayList<String>();
                Int2ObjectOpenHashMap _neigbhor=brNeigbhor.getValue();
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

        class getReduceKey implements PairFunction<String,String,String> {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tmpStr=s.split(",");
                return new Tuple2<String, String>(tmpStr[0]+"-"+tmpStr[1],s);
            }
        }

        class reduceForEdge implements Function2<String,String,String> {
            @Override
            public String call(String s, String s2) throws Exception {
                return s;
            }
        }

        class getEdgeData implements Function<Tuple2<String,String>,String> {
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

    public static void initial3V(JavaSparkContext sc,String savePath,
                                 Int2ObjectOpenHashMap neigbhor,String exInPath,String exSavePath,int partNum) throws IOException {

        final Broadcast<Int2ObjectOpenHashMap> brNeigbhor=sc.broadcast(neigbhor);
        class calEdgeDist implements Function<String,String>{
            @Override
            public String call(String s) throws Exception {
                Int2ObjectOpenHashMap _neigbhor=brNeigbhor.getValue();
                String[] tmpStr=s.split(",");
                int x=Integer.parseInt(tmpStr[0]);
                int y=Integer.parseInt(tmpStr[1]);
                String retVaule="";
                float xyDist=calDistance(x,y,0,_neigbhor);
                retVaule=x+","+y+","+xyDist;
                return retVaule;
            }
        }

        sc.textFile(savePath+"/"+exInPath).repartition(partNum).map(new calEdgeDist()).saveAsTextFile(savePath+"/"+exSavePath);
        brNeigbhor.unpersist();
    }

    public static void main(String[] args) throws IOException {

        String inputPath=args[0];
        String outputPath=args[1];
        int _start=Integer.parseInt(args[2]);
        int partNum=Integer.parseInt(args[3]);
        //String exInPath=args[5];
        //String exSavePath=args[6];

        SparkConf conf=new SparkConf().setAppName("InitialNet_EX");//.setMaster(master).setJars(jarPathArray);

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[]{Int2ObjectOpenHashMap.class,Int2FloatOpenHashMap.class,IntOpenHashSet.class,ObjectOpenHashSet.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        Int2ObjectOpenHashMap neigbhor=loadingEdge(inputPath);

        for (int i = _start; i <50 ; i++) {
            String exInPath="tmp_result/"+i;
            String exSavePath="exEdge/"+i;
            initial3V(sc,outputPath,neigbhor,exInPath,exSavePath,partNum);
        }
        //initial3V(sc,outputPath,neigbhor,partNum_2,exInPath,exSavePath);
    }
}
