package CommunityDetection;

/**
 * Created by hyy on 2020/1/7.
 */
public class DistanceMatrix {

    private double [] distanceMatrix;//=Matrix.Factory.zeros(a.size(),b.size());
    private int data_N;

    public DistanceMatrix(int data_N){
        distanceMatrix=new double[(data_N*(data_N-1))/2];
        data_N=data_N;
    }

    public void set_element(int x,int y,double value){
        if(x==y){
            return;
        }else {
            int row=x;
            int clo=y;

            if (x>y){
                row=y;
                clo=x;
            }

            int index=((data_N-1+data_N-row)*row)/2+(clo-row)-1;
            distanceMatrix[index]=value;

        }
    }

    public double get_element(int x,int y){
        if(x==y){
            return 0;
        }else {
            int row=x;
            int clo=y;

            if (x>y){
                row=y;
                clo=x;
            }

            int index=((data_N-1+data_N-row)*row)/2+(clo-row)-1;
            return distanceMatrix[index];
        }
    }
}
