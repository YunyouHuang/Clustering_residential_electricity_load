package CommunityDetection;

import java.util.ArrayList;

/**
 * Created by hyy on 2017/9/15.
 */
public class TimeSeries {
    public int mark;
    public ArrayList<Double> data;

    public void setMark(int _mark){
        mark=_mark;
    }

    public int getMark(){
        return mark;
    }

    public void setData(ArrayList<Double> _data){
        ArrayList<Double> tmpData=new ArrayList<Double>();
        tmpData.addAll(_data);
        data=tmpData;
    }

    public ArrayList<Double> getData(){
        ArrayList<Double> tmpData=new ArrayList<Double>();
        tmpData.addAll(data);
        return tmpData;
    }

    public TimeSeries(int _mark,ArrayList<Double> _data){
        mark=_mark;
        ArrayList<Double> tmpData=new ArrayList<Double>();
        tmpData.addAll(_data);
        data=tmpData;
    }
}
