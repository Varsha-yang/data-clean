package com.kdc.dataclean.job.config;

import java.util.HashMap;
import java.util.Map;

public class Unit {
    /**
     * 文化程度
     */
    public static Map<String,String> educDeg(){
        Map<String,String> map = new HashMap<>();
        map.put("GRA","11"); // 博士
        map.put("BAC","21"); // 本科
        map.put("COL","31"); // 专科
        map.put("SEC","41"); // 中专
        map.put("TEC","47"); // 技工学校
        map.put("SHS","61"); // 高中
        map.put("JHS","71"); // 初中
        map.put("PRS","81"); // 小学
        map.put("ILL","90"); // 文盲与半文盲
        return map;
    }
    /**
     * 政治面貌
     */
    public static Map<String,String> polctlSt(){
        Map<String,String> map = new HashMap<>();
        map.put("ZD","01"); // 中国共产党党员
        map.put("ZY","02"); // 中国共产党预备党员
        map.put("GQ","03"); // 中国共产主义青年团团员
        map.put("MG","04"); // 中国国民党革命委员会会员
        map.put("MM","05"); // 中国民主同盟盟员
        map.put("MJ","06"); // 中国民主建国会会员
        map.put("MH","07"); // 中国民主促进会会员
        map.put("NG","08"); // 中国农工民主党党员
        map.put("ZG","09"); // 中国致公党党员
        map.put("XS","10"); // 九三学社社员
        map.put("TM","11"); // 台湾民主自治同盟盟员
        map.put("WD","12"); // 无党派民主人士
        map.put("QZ","13"); // 群众
        map.put("OT","99"); // 其他
        return map;
    }

    /**
     * 婚姻状况
     */
    public static Map<String,String> maritalSt(){
        Map<String,String> map = new HashMap<>();
        map.put("UM","10"); // 未婚
        map.put("MA","20"); // 已婚
        map.put("WI","30"); // 丧偶
        map.put("DI","40"); // 离异
        return map;
    }

    /**
     * 服兵役情况
     */
    public static Map<String,String> militarySvcSt(){
        Map<String,String> map = new HashMap<>();
        map.put("NO","0"); // 未服兵役
        map.put("ON","4"); // 服现役
        map.put("RE","2"); // 预备役
        map.put("OU","1"); // 退出现役
        return map;
    }

    /**
     * 职业类型代码
     */
    public static Map<String,String> coyClass(){
        Map<String,String> map = new HashMap<>();
        map.put("NO","0"); // 未服兵役
        map.put("ON","4"); // 服现役
        map.put("RE","2"); // 预备役
        map.put("OU","1"); // 退出现役
        return map;
    }

}
