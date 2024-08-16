/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package clustering;

import java.util.ArrayList;
import java.util.HashMap;

/**
 *
 * @author hyy
 */
public class test {
    public static void main(String[] args) {
        HashMap<Integer,ArrayList<Integer>> kkHashMap=new HashMap<Integer,ArrayList<Integer>>();
        HashMap<String,ArrayList<Integer>> xxHashMap=new HashMap<String,ArrayList<Integer>>();
        
        
        ArrayList<Integer> a=new ArrayList<Integer>();
        xxHashMap.put("1", a);
        kkHashMap.put(1, a);
        
        ArrayList<Integer> s=kkHashMap.get(1);
        s.add(5);
        s.add(4);
        s.add(3);
        s.add(2);
        
        System.out.println(xxHashMap.get("1").size());
                
    }
    
}
