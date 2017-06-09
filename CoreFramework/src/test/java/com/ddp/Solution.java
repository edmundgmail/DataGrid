package com.ddp;

import java.io.*;
import java.util.*;
import java.text.*;
import java.math.*;
import java.util.regex.*;
/**
 * Created by eguo on 5/1/17.
 */
public class Solution {
    static int counting(String s) {
        List<Pattern> searchPatterns = new ArrayList<>();
        for(int i =1; i <= s.length()/2; i++){
            String zero = new String(new char[i]).replace("\0","0");
            String one = new String(new char[i]).replace("\0","1");
            searchPatterns.add(Pattern.compile(zero + one));
            searchPatterns.add(Pattern.compile(one + zero));
        }

        int count = 0;

        for(Pattern pattern : searchPatterns){
            Matcher matcher = pattern.matcher(s);
            while(matcher.find()){
                count++;
            }
        }

        return count;
    }

    public static void main(String[] args) {

        String s ="10101";
        System.out.println(counting(s));

    }
}
