package CommunityDetection;

import com.FileOperate.FileWrite;

import java.text.DecimalFormat;

/**
 * Created by hyy on 2020/1/15.
 */
public class Combine_index {
    public static void main(String[] args) {

        // TODO Auto-generated method stub
        //String dataPath=args[0];
        //String centerPath=args[1];
        //int win=Integer.parseInt(args[2]);
        //String savePath=args[2];


        String data_path="/home/huangyunyou/community/20200102/cluster_result/";
        int win=2;

        /*
        SF_EU sf=new SF_EU();

        for (int i = 0; i < 99; i++) {

            System.out.println(" Processing index : "+i);
            String cfsfdp_data=data_path+"CFSFDP/CFSFDP_"+i+"/data";
            String cfsfdp_center=data_path+"CFSFDP/CFSFDP_"+i+"/centers/centers.txt";

            String ac_data=data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/data";
            String ac_center=data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/centers/centers.txt";

            String gm_data=data_path+"GaussianMixture/GaussianMixture_"+i+"/data";
            String gm_center=data_path+"GaussianMixture/GaussianMixture_"+i+"/centers/centers.txt";

            String km_data=data_path+"KMeans/KMeans_"+i+"/data";
            String km_center=data_path+"KMeans/KMeans_"+i+"/centers/centers.txt";

            String sc_data=data_path+"SpectralClustering/SpectralClustering_"+i+"/data";
            String sc_center=data_path+"SpectralClustering/SpectralClustering_"+i+"/centers/centers.txt";

            String ke_data=data_path+"kmediods/kmediods_"+i+"/data";
            String ke_center=data_path+"kmediods/kmediods_"+i+"/centers/centers.txt";


            double resolution = 1.0 - (0.005 * i);

            DecimalFormat dfr = new DecimalFormat("#.000");
            String rString = dfr.format(resolution);

            String our_data=data_path+"result_2v/r_3_050_1_1_" + rString;
            String our_center=data_path+"result_2v/r_3_050_1_1_" + rString+"centers/centers.txt";

            //String w=evaluation(dataPath,centerPath);
            //System.out.println(dataPath);
            //System.out.print(w);
            FileWrite.WriteTxt(DBI_EU.evaluation(cfsfdp_data,cfsfdp_center),data_path+"dbi_cfsfdp_20200115.txt");
            FileWrite.WriteTxt(DBI_EU.evaluation(ac_data,ac_center),data_path+"dbi_ac_20200115.txt");
            FileWrite.WriteTxt(DBI_EU.evaluation(gm_data,gm_center),data_path+"dbi_gm_20200115.txt");
            FileWrite.WriteTxt(DBI_EU.evaluation(km_data,km_center),data_path+"dbi_km_20200115.txt");
            FileWrite.WriteTxt(DBI_EU.evaluation(sc_data,sc_center),data_path+"dbi_sc_20200115.txt");
            FileWrite.WriteTxt(DBI.evaluation(ke_data,ke_center,win),data_path+"dbi_ke_20200115.txt");
            FileWrite.WriteTxt(DBI.evaluation(our_data,our_center,win),data_path+"dbi_our_20200115.txt");


            FileWrite.WriteTxt(VCN_EU.evaluation(cfsfdp_data,cfsfdp_center),data_path+"vcn_cfsfdp_20200115.txt");
            FileWrite.WriteTxt(VCN_EU.evaluation(ac_data,ac_center),data_path+"vcn_ac_20200115.txt");
            FileWrite.WriteTxt(VCN_EU.evaluation(gm_data,gm_center),data_path+"vcn_gm_20200115.txt");
            FileWrite.WriteTxt(VCN_EU.evaluation(km_data,km_center),data_path+"vcn_km_20200115.txt");
            FileWrite.WriteTxt(VCN_EU.evaluation(sc_data,sc_center),data_path+"vcn_sc_20200115.txt");
            FileWrite.WriteTxt(VCN.evaluation(ke_data,ke_center,win),data_path+"vcn_ke_20200115.txt");
            FileWrite.WriteTxt(VCN.evaluation(our_data,our_center,win),data_path+"vcn_our_20200115.txt");



            FileWrite.WriteTxt(sf.evaluation(cfsfdp_data,cfsfdp_center),data_path+"sf_cfsfdp_20200115.txt");
            FileWrite.WriteTxt(sf.evaluation(ac_data,ac_center),data_path+"sf_ac_20200115.txt");
            FileWrite.WriteTxt(sf.evaluation(gm_data,gm_center),data_path+"sf_gm_20200115.txt");
            FileWrite.WriteTxt(sf.evaluation(km_data,km_center),data_path+"sf_km_20200115.txt");
            FileWrite.WriteTxt(sf.evaluation(sc_data,sc_center),data_path+"sf_sc_20200115.txt");
            FileWrite.WriteTxt(SF.evaluation(data_path+"dataSetCenter.txt",ke_data,ke_center,win),data_path+"sf_ke_20200115.txt");
            FileWrite.WriteTxt(SF.evaluation(data_path+"dataSetCenter.txt",our_data,our_center,win),data_path+"sf_our_20200115.txt");

            Entropy_2v entropy_cfsfdp=new Entropy_2v();
            FileWrite.WriteTxt(entropy_cfsfdp.evaluation(cfsfdp_data,0,0.0),data_path+"entropy_cfsfdp_20200115.txt");

            Entropy_2v entropy_ac=new Entropy_2v();
            FileWrite.WriteTxt(entropy_ac.evaluation(ac_data,0,0.0),data_path+"entropy_ac_20200115.txt");

            Entropy_2v entropy_gm=new Entropy_2v();
            FileWrite.WriteTxt(entropy_gm.evaluation(gm_data,0,0.0),data_path+"entropy_gm_20200115.txt");

            Entropy_2v entropy_km=new Entropy_2v();
            FileWrite.WriteTxt(entropy_km.evaluation(km_data,0,0.0),data_path+"entropy_km_20200115.txt");

            Entropy_2v entropy_sc=new Entropy_2v();
            FileWrite.WriteTxt(entropy_sc.evaluation(sc_data,0,0.0),data_path+"entropy_sc_20200115.txt");

            Entropy_2v entropy_ke=new Entropy_2v();
            FileWrite.WriteTxt(entropy_ke.evaluation(ke_data,0,0.0),data_path+"entropy_ke_20200115.txt");

            Entropy_2v entropy_our=new Entropy_2v();
            FileWrite.WriteTxt(entropy_our.evaluation(our_data,0,0.0),data_path+"entropy_our_20200115.txt");

        }
        */

        COP_EU cop_eu =new COP_EU("/home/huangyunyou/community/20200102/norBySum_filter");
        for (int i = 0; i < 99; i++) {
            String cfsfdp_data=data_path+"CFSFDP/CFSFDP_"+i+"/data";
            String cfsfdp_center=data_path+"CFSFDP/CFSFDP_"+i+"/centers/centers.txt";

            String ac_data=data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/data";
            String ac_center=data_path+"AgglomerativeClustering/AgglomerativeClustering_"+i+"/centers/centers.txt";

            String gm_data=data_path+"GaussianMixture/GaussianMixture_"+i+"/data";
            String gm_center=data_path+"GaussianMixture/GaussianMixture_"+i+"/centers/centers.txt";

            String km_data=data_path+"KMeans/KMeans_"+i+"/data";
            String km_center=data_path+"KMeans/KMeans_"+i+"/centers/centers.txt";

            String sc_data=data_path+"SpectralClustering/SpectralClustering_"+i+"/data";
            String sc_center=data_path+"SpectralClustering/SpectralClustering_"+i+"/centers/centers.txt";

            FileWrite.WriteTxt(cop_eu.evaluation(cfsfdp_data,cfsfdp_center),data_path+"cop_cfsfdp_20200115.txt");
            FileWrite.WriteTxt(cop_eu.evaluation(ac_data,ac_center),data_path+"cop_ac_20200115.txt");
            FileWrite.WriteTxt(cop_eu.evaluation(gm_data,gm_center),data_path+"cop_gm_20200115.txt");
            FileWrite.WriteTxt(cop_eu.evaluation(km_data,km_center),data_path+"cop_km_20200115.txt");
            FileWrite.WriteTxt(cop_eu.evaluation(sc_data,sc_center),data_path+"cop_sc_20200115.txt");

        }
        cop_eu=null;

        COP cop=new COP("/home/huangyunyou/community/20200102/norBySum_filter",win);
        for (int i = 0; i < 99; i++) {

            String ke_data=data_path+"kmediods/kmediods_"+i+"/data";
            String ke_center=data_path+"kmediods/kmediods_"+i+"/centers/centers.txt";


            double resolution = 1.0 - (0.005 * i);

            DecimalFormat dfr = new DecimalFormat("#.000");
            String rString = dfr.format(resolution);

            String our_data=data_path+"result_2v/r_3_050_1_1_" + rString;
            String our_center=data_path+"result_2v/r_3_050_1_1_" + rString+"centers/centers.txt";
            FileWrite.WriteTxt(cop.evaluation(ke_data,ke_center,win),data_path+"cop_ke_20200115.txt");
            FileWrite.WriteTxt(cop.evaluation(our_data,our_center,win),data_path+"cop_our_20200115.txt");

        }

    }
}
