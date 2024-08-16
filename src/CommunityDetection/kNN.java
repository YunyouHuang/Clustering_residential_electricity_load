package CommunityDetection;

import com.Data.DataOperation;
import com.Similarity.Distance;
import com.Transform.FFT;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hyy on 2017/9/15.
 */
public class kNN {

    public static ArrayList<TimeSeries> roadData(String path){
        ArrayList<TimeSeries> retValue=new ArrayList<TimeSeries>();
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmp=line.split(",");
                int tmpMark=Integer.parseInt(tmp[0]);
                ArrayList<Double> tmpArry=new ArrayList<Double>();
                for (int i = 1; i < tmp.length; i++) {
                    tmpArry.add(Double.parseDouble(tmp[i]));
                }
                retValue.add(new TimeSeries(tmpMark,tmpArry));
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
        return retValue;
    }

    public static String kNNRun(String trainPath,String testPath,String dataSetName,double g,int thrCount) throws InterruptedException {
        ArrayList<TimeSeries> trainData=roadData(trainPath);
        ArrayList<TimeSeries> testData=roadData(testPath);
        //Distance dist=new Distance();
        int[] countArray=new int[5];
        for (int i = 0; i < 5; i++) {
            countArray[i]=0;
        }

        int countData=0;
        kNN _knn=new kNN();
        while (countData<testData.size()){
            ExecutorService exe = Executors.newFixedThreadPool(thrCount);
            for (int i = 0; i < thrCount; i++) {
                if (countData<testData.size()){
                    CalThread calThr=_knn.new CalThread("Thr-"+i,trainData,testData.get(countData),countArray,g);
                    exe.execute(calThr);
                    countData++;
                }
            }
            exe.shutdown();
            while (true) {
                if (exe.isTerminated()) {
                    System.out.println(countData+"结束了！");
                    break;
                }
                Thread.sleep(200);
            }
        }

        String retValue=dataSetName;
        System.out.println("多线程计算结束了！");
        //region  计算测测试集的标号
        /*
        int rCount_D=0;
        int rCount_E=0;
        int rCount_S=0;
        int rCount_WD=0;
        for (int i = 0; i < testData.size(); i++) {
            TimeSeries tmpTT=testData.get(i);
            double _min_D=Double.MAX_VALUE;
            double _min_E=Double.MAX_VALUE;
            double _min_S=Double.MAX_VALUE;
            double _min_WD=Double.MAX_VALUE;

            int mark_D=-1;
            int mark_E=-1;
            int mark_S=-1;
            int mark_WD=-1;

            for (int j = 0; j < trainData.size(); j++) {
                TimeSeries tmpTN=trainData.get(j);
                double _D=dist.getDTWDistanceForDouble(tmpTN.data,tmpTT.data);
                double _E=dist.getEuclideanDistance(tmpTN.data,tmpTT.data);
                double _S=FFT.getSDB(tmpTN.data,tmpTT.data)._2;
                double _WD=dist.getWDTWDistanceForDouble(tmpTN.data,tmpTT.data,g);

                if (_D<_min_D){
                    _min_D=_D;
                    mark_D=tmpTN.mark;
                }

                if (_E<_min_E){
                    _min_E=_E;
                    mark_E=tmpTN.mark;
                }

                if (_S<_min_S){
                    _min_S=_S;
                    mark_S=tmpTN.mark;
                }

                if (_WD<_min_WD){
                    _min_WD=_WD;
                    mark_WD=tmpTN.mark;
                }
            }

            if (mark_D==tmpTT.mark){
                rCount_D++;
            }

            if (mark_E==tmpTT.mark){
                rCount_E++;
            }

            if (mark_S==tmpTT.mark){
                rCount_S++;
            }

            if (mark_WD==tmpTT.mark){
                rCount_WD++;
            }
        }
        */
        //endregion

        System.out.println("\n\n****************************\n");
        System.out.println(dataSetName);
        System.out.println("Train_size  "+trainData.size()+"       Test_size  "+testData.size()+"        Time_series_length  "+testData.get(0).data.size());
        retValue+=","+countArray[0]/(double)testData.size();
        retValue+=","+countArray[1]/(double)testData.size();
        retValue+=","+countArray[2]/(double)testData.size();
        retValue+=","+countArray[3]/(double)testData.size();
        retValue+=","+countArray[4]/(double)testData.size();


        System.out.println("DTW  "+countArray[0]/(double)testData.size());
        System.out.println("EU  "+countArray[1]/(double)testData.size());
        System.out.println("SDB  "+countArray[2]/(double)testData.size());
        System.out.println("WDTW  "+countArray[3]/(double)testData.size());
        System.out.println("SWDTW  "+countArray[4]/(double)testData.size());

        return retValue;
    }

    public static void main(String[] args) throws InterruptedException {

        String path=args[0];
        String name=args[1];
        double g=Double.parseDouble(args[2]);
        int thr=Integer.parseInt(args[3]);
        String file=path+"/"+name+"/"+name;
        //String file="/Users/hyy/Desktop/communityDetection/timeSeriesDataset/UCR_TS_Archive_2015/50words/50words";
        kNNRun(file+"_TRAIN",file+"_TEST",name,g,thr);
    }

    class CalThread extends Thread {
        private ArrayList<TimeSeries> _trainData;
        private TimeSeries _test;
        private int[] _countArray;
        double _g;

        public CalThread(String name,ArrayList<TimeSeries> trainData,TimeSeries test,int[] countArray,double g){
            super(name);
            _trainData=trainData;
            _test=test;
            _countArray=countArray;
            _g=g;
        }
        public void run(){

            //System.out.println(this.getName());
            TimeSeries tmpTT=_test;
            double _min_D=Double.MAX_VALUE;
            double _min_E=Double.MAX_VALUE;
            double _min_S=Double.MAX_VALUE;
            double _min_WD=Double.MAX_VALUE;
            double _min_SWD=Double.MAX_VALUE;

            int mark_D=-1;
            int mark_E=-1;
            int mark_S=-1;
            int mark_WD=-1;
            int mark_SWD=-1;
            Distance dist=new Distance();
            for (int j = 0; j < _trainData.size(); j++) {
                TimeSeries tmpTN=_trainData.get(j);
                double _D=dist.getDTWDistanceForDouble(tmpTN.data,tmpTT.data);
                double _E=dist.getEuclideanDistance(tmpTN.data,tmpTT.data);
                double _S=FFT.getSDB(tmpTN.data,tmpTT.data)._2;
                double _WD=dist.getWDTWDistanceForDouble(tmpTN.data,tmpTT.data,_g);
                double _SWD=dist.get_Shift_Punishment_DTWDistanceFor(DataOperation.arrayDouble2Float(tmpTT.data),DataOperation.arrayDouble2Float(tmpTN.data),0.85);
                if (_D<_min_D){
                    _min_D=_D;
                    mark_D=tmpTN.mark;
                }

                if (_E<_min_E){
                    _min_E=_E;
                    mark_E=tmpTN.mark;
                }

                if (_S<_min_S){
                    _min_S=_S;
                    mark_S=tmpTN.mark;
                }

                if (_WD<_min_WD){
                    _min_WD=_WD;
                    mark_WD=tmpTN.mark;
                }

                if (_SWD<_min_SWD){
                    _min_SWD=_SWD;
                    mark_SWD=tmpTN.mark;
                }
            }

            synchronized (_countArray){
                if (mark_D==tmpTT.mark){
                    _countArray[0]++;
                }

                if (mark_E==tmpTT.mark){
                    _countArray[1]++;
                }

                if (mark_S==tmpTT.mark){
                    _countArray[2]++;
                }

                if (mark_WD==tmpTT.mark){
                    _countArray[3]++;
                }

                if (mark_SWD==tmpTT.mark){
                    _countArray[4]++;
                }
            }

        }
    }
}
