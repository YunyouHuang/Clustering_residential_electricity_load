package CommunityDetection;

import com.Data.DataOperation;
import com.Similarity.Distance;
import com.Transform.FFT;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by hyy on 2017/10/12.
 */
public class KNNForTest {

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



    public static double kNNRun(ArrayList<TimeSeries> trainData,ArrayList<TimeSeries> testData,int thrCount,double best_g) throws InterruptedException {
        //ArrayList<TimeSeries> trainData=roadData(trainPath);
        //ArrayList<TimeSeries> testData=roadData(testPath);
        //Distance dist=new Distance();
        int[] countArray=new int[1];
        for (int i = 0; i < 1; i++) {
            countArray[i]=0;
        }

        int countData=0;
        KNNForTest _knn=new KNNForTest();
        //double best_g=1.0;

        while (countData<testData.size()){
            ExecutorService exe = Executors.newFixedThreadPool(thrCount);
            for (int i = 0; i < thrCount; i++) {
                if (countData<testData.size()){
                    KNNForTest.CalThread calThr=_knn.new CalThread("Thr-"+i,trainData,testData.get(countData),countArray,best_g);
                    exe.execute(calThr);
                    countData++;
                }
            }
            exe.shutdown();
            while (true) {
                if (exe.isTerminated()) {
                    //System.out.println(countData+"结束了！");
                    break;
                }
                Thread.sleep(200);
            }
        }

        //String retValue=dataSetName;
        //System.out.println("多线程计算结束了！");
        //System.out.println("\n\n****************************\n");
        //System.out.println(dataSetName);
        //System.out.println("Train_size  "+trainData.size()+"       Test_size  "+testData.size()/2+"        Time_series_length  "+testData.get(0).data.size());
        return countArray[0]/(double)testData.size();
        //System.out.println("SWDTW  "+countArray[0]/(double)testData.size());
        //return retValue;
    }

    public static void main(String[] args) throws InterruptedException {

        String path=args[0];
        String name=args[1];
        //double g=Double.parseDouble(args[2]);
        int thr=Integer.parseInt(args[2]);
        double gradient=Double.parseDouble(args[3]);
        double _max_g=Double.parseDouble(args[4]);
        double _min_g=Double.parseDouble(args[5]);

        String file=path+"/"+name+"/"+name;
        //String file="/Users/hyy/Desktop/communityDetection/timeSeriesDataset/UCR_TS_Archive_2015/50words/50words";
        ArrayList<TimeSeries> trainData=roadData(file+"_TRAIN");
        ArrayList<TimeSeries> testData_All=roadData(file+"_TEST");
        ArrayList<TimeSeries> _td_p1=new ArrayList<TimeSeries>();
        ArrayList<TimeSeries> _td_p2=new ArrayList<TimeSeries>();

        for (int i = 0; i < testData_All.size(); i++) {
            if (i<testData_All.size()/2){
                _td_p1.add(testData_All.get(i));
            }else {
                _td_p2.add(testData_All.get(i));
            }
        }

        double _best_g=_max_g;
        double _bestRate=0;
        double g=_max_g;

        while (g>=_min_g){
            double tmp_rate=kNNRun(trainData,_td_p1,thr,g);
            System.out.println("g  "+g+"      tmp_rate    "+tmp_rate);
            if (tmp_rate>_bestRate){
                _bestRate=tmp_rate;
                _best_g=g;
            }
            g=g-gradient;
        }

        System.out.println("********************************");
        System.out.println("********************************");
        System.out.println("********************************");
        System.out.println(name);
        System.out.println("_best_g  "+_best_g+"      tmp_rate    "+_bestRate);
        double rate=kNNRun(trainData,_td_p2,thr,_best_g);
        System.out.println("_best_g  "+_best_g+"      tmp_rate    "+rate);

        //kNNRun(file+"_TRAIN",file+"_TEST",name,thr);
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
            double _min_SWD=Double.MAX_VALUE;

            int mark_SWD=-1;

            Distance dist=new Distance();
            for (int j = 0; j < _trainData.size(); j++) {
                TimeSeries tmpTN=_trainData.get(j);
                //double _D=dist.getDTWDistanceForDouble(tmpTN.data,tmpTT.data);
                double _SWD=dist.get_Shift_Punishment_DTWDistanceFor(DataOperation.arrayDouble2Float(tmpTT.data),DataOperation.arrayDouble2Float(tmpTN.data),_g);
                if (_SWD<_min_SWD){
                    _min_SWD=_SWD;
                    mark_SWD=tmpTN.mark;
                }

            }

            synchronized (_countArray){
                if (mark_SWD==tmpTT.mark){
                    _countArray[0]++;
                }

            }

        }
    }
}
