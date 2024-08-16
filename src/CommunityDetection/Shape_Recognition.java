package CommunityDetection;

import JavaTuple.Tuple3IFP;
import com.Chart.LineChart;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RefineryUtilities;

import javax.jdo.annotations.Index;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by hyy on 2017/9/19.
 */
public class Shape_Recognition {


    public static ArrayList<Integer> firstNorDifference(ArrayList<Float> data){
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        if (data.size()>1){
            for (int i = 1; i < data.size(); i++) {
                float pre=data.get(i-1);
                float cur=data.get(i);
                if (cur>pre){
                    retValue.add(1);
                }else if (cur==pre){
                    retValue.add(0);
                }else {
                    retValue.add(-1);
                }
            }
            retValue.add(0);
        }else {
            System.out.println("The data's length must bigger than 1!");
        }
        return retValue;
    }

    public static ArrayList<Integer> secondNorDifference(ArrayList<Integer> data){
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        if (data.size()>1){
            retValue.add(0);
            for (int i = 1; i < data.size(); i++) {
                int pre=data.get(i-1);
                int cur=data.get(i);
                retValue.add(cur-pre);
            }
        }else {
            System.out.println("The data's length must bigger than 1!");
        }
        return retValue;
    }

    public static float getMeansArray(ArrayList<Float> data){
        float sum=0;
        for (int i = 0; i < data.size(); i++) {
            sum+=data.get(i);
        }
        float mean=sum/data.size();

        return mean;
    }

    public static ArrayList<Integer> getBreakpoint(ArrayList<Float> data){
        ArrayList<Integer> _index=new ArrayList<Integer>();
        ArrayList<Float> _max_min=new ArrayList<Float>();
        findMaxAndMinIndex(data,_index,_max_min);

        ArrayList<Integer> prominent_index=new ArrayList<Integer>();
        ArrayList<Float> prominent_max_min=new ArrayList<Float>();

        ArrayList<Float> _aMaxMin=findAverageRiseAndFall(_index,_max_min);
        for (int i = 0; i < _max_min.size(); i++) {
            if ((_max_min.get(i)>(_aMaxMin.get(0)*0.3))||(_max_min.get(i)<(_aMaxMin.get(1)*0.3))){
                prominent_max_min.add(_max_min.get(i));
                prominent_index.add(_index.get(i));
            }
        }

        float means=getMeansArray(data);


        if (prominent_index.size()>1){
            for (int i = 1; i < prominent_max_min.size(); i++) {
                if (prominent_max_min.get(i)*prominent_max_min.get(i-1)>0){
                    prominent_index.set(i-1,-1);
                }
            }
        }


        ArrayList<Integer> finalIndex=new ArrayList<>();
        int preIndex=0;
        for (int i = 0; i < prominent_index.size(); i++) {
            if (prominent_index.get(i)>0){
                finalIndex.add(preIndex);
                int _maxIndex=findMaxIndex(data,preIndex,prominent_index.get(i));
                int _minIndex=findMinIndex(data,preIndex,prominent_index.get(i));
                if (_maxIndex>_minIndex){
                    finalIndex.add(_minIndex);
                    finalIndex.add(_maxIndex);
                }else {
                    finalIndex.add(_maxIndex);
                    finalIndex.add(_minIndex);
                }
                preIndex=prominent_index.get(i);
            }
        }
        finalIndex.add(preIndex);
        if (preIndex<(data.size()-1)){
            int _maxIndex=findMaxIndex(data,preIndex,data.size()-1);
            int _minIndex=findMinIndex(data,preIndex,data.size()-1);
            if (_maxIndex>_minIndex){
                finalIndex.add(_minIndex);
                finalIndex.add(_maxIndex);
            }else {
                finalIndex.add(_maxIndex);
                finalIndex.add(_minIndex);
            }
            finalIndex.add(data.size()-1);
        }
        ArrayList<Integer> _final=new ArrayList<Integer>();
        //System.out.println("Prominent breakpoint :");
        //System.out.print(0+"    ");
        _final.add(0);
        for (int i = 1; i <finalIndex.size(); i++) {
            if (finalIndex.get(i)!=finalIndex.get(i-1)){
                _final.add(finalIndex.get(i));
                //System.out.print(finalIndex.get(i)+"    ");
            }
        }

        ArrayList<Integer> retValue=new ArrayList<Integer>();
        for (int i = 1; i <_final.size() ; i++) {
            int preTmpIndex=_final.get(i-1);
            int curTmpIndex=_final.get(i);
            if (data.get(preTmpIndex)>=data.get(curTmpIndex)){
                if (data.get(preTmpIndex)>(means*0.3)) {
                    int endIndexPoint = preTmpIndex;
                    while ((endIndexPoint <= curTmpIndex) && (data.get(endIndexPoint) > (means * 0.3))) {
                        endIndexPoint++;
                    }
                    retValue.add(preTmpIndex);
                    if (endIndexPoint > curTmpIndex) {
                        retValue.add(curTmpIndex);
                    } else {
                        retValue.add(endIndexPoint);
                    }
                }
            }else {
                if (data.get(curTmpIndex)>(means*0.3)) {
                    int startIndexPoint = curTmpIndex;
                    while ((startIndexPoint >= preTmpIndex) && (data.get(startIndexPoint) > (means * 0.3))) {
                        startIndexPoint--;
                    }
                    if (startIndexPoint < preTmpIndex) {
                        retValue.add(preTmpIndex);
                    } else {
                        retValue.add(startIndexPoint);
                    }
                    retValue.add(curTmpIndex);
                }
            }
        }

        //System.out.println("\nSpace breakpoint :");
        //System.out.print(retValue.get(0)+"    ");
        //for (int i = 0; i <retValue.size(); i++) {
           // System.out.print(retValue.get(i)+"    ");
        //}

        return retValue;
    }
    
    public static int findMaxIndex(ArrayList<Float> data,int _start ,int _end){
        float _max=data.get(_start);
        int retIndex=_start;
        for (int i = (_start+1); i < (_end+1); i++) {
            if (data.get(i)>=_max){
                _max=data.get(i);
                retIndex=i;
            }
        }
        return retIndex;
    }

    public static int findMinIndex(ArrayList<Float> data,int _start ,int _end){
        float _max=data.get(_start);
        int retIndex=_start;
        for (int i = (_start+1); i < (_end+1); i++) {
            if (data.get(i)<_max){
                _max=data.get(i);
                retIndex=i;
            }
        }
        return retIndex;
    }

    public static void findMaxAndMinIndex(ArrayList<Float> data,ArrayList<Integer> maxAndMinIndex,ArrayList<Float> maxMin){
        int i=1;
        while (i<data.size()){
            if ((i<data.size())&&(data.get(i)>=data.get(i-1))){
                float raise=0;
                while (data.get(i)>=data.get(i-1)){
                    raise+=(data.get(i)-data.get(i-1));
                    i++;
                    if (i>=data.size()){
                        break;
                    }
                }
                maxMin.add(raise);
                maxAndMinIndex.add(i-1);
            }

            if ((i<data.size()&&(data.get(i)<data.get(i-1)))){
                float fall=0;
                while (data.get(i)<=data.get(i-1)){
                    fall+=(data.get(i)-data.get(i-1));
                    i++;
                    if (i>=data.size()){
                        break;
                    }
                }
                maxAndMinIndex.add(i-1);
                maxMin.add(fall);
            }
        }
    }

    public static ArrayList<Float> findAverageRiseAndFall(ArrayList<Integer> maxAndMinIndex,ArrayList<Float> maxMin){
        float tRaise=0;
        int noOfRaise=0;
        float tFall=0;
        int noOfFall=0;
        for (int i = 0; i < maxMin.size(); i++) {
            if (maxMin.get(i)>=0){
                tRaise+=maxMin.get(i);
                noOfRaise++;
            }
            if (maxMin.get(i)<0){
                tFall+=maxMin.get(i);
                noOfFall++;
            }
        }
        ArrayList<Float> retValue=new ArrayList<Float>();
        retValue.add(tRaise/noOfRaise);
        retValue.add(tFall/noOfFall);
        return retValue;
    }

    public static void main(String[] args)  {
        String a="0,0,0,0,0,0,1,3,5,6,7.5,8.3,9,9,9,8.3,7.5,6,5,3,1,0,0,0,3,3,4,5,3,6,7,0,0,0";
        ArrayList<Float> x=new ArrayList<Float>();
        XYSeriesCollection sc=new XYSeriesCollection();
        XYSeries s1 = new XYSeries("x");
        XYSeries fn = new XYSeries("fn");
        XYSeries sn = new XYSeries("sn");

        String[] aa=a.split(",");
        for (int i = 0; i < aa.length; i++) {
            x.add(Float.parseFloat(aa[i]));
            s1.add(i,Double.parseDouble(aa[i])+3);
        }

        getBreakpoint(x);
        ArrayList<Integer> fArray=firstNorDifference(x);
        ArrayList<Integer> sArray=secondNorDifference(fArray);

        for (int i = 0; i < fArray.size(); i++) {
            fn.add(i,fArray.get(i));
        }

        for (int i = 0; i < sArray.size(); i++) {
            sn.add(i,sArray.get(i));
        }
        sc.addSeries(s1);
        sc.addSeries(fn);
        sc.addSeries(sn);

        LineChart lChart=new LineChart("Electricity consumption");
        XYDataset dataSet=sc;
        ArrayList<Tuple3IFP> style=new ArrayList<Tuple3IFP>();
        for (int i = 0; i < dataSet.getSeriesCount(); i++) {
            style.add(new Tuple3IFP(1,5f,null));
        }

        // lChart.drawLine2D("测试", "xxxx", "people", dataset, flag, true);
        //lChart.drawLine2DXY(cluNum,"No clusters","Assessment value",dataSet,style);
        lChart.drawLine2DXY("DTW","Time/Quarter","Consumption/KWh",dataSet,style,-1);
        lChart.pack( );
        RefineryUtilities.centerFrameOnScreen( lChart );
        lChart.setVisible( true );

    }
}
