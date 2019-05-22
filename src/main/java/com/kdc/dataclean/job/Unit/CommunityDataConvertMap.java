package com.kdc.dataclean.job.Unit;

import java.util.HashMap;
import java.util.Map;

public class CommunityDataConvertMap {


    /**
     * 学历
     */
    public static String educDeg(String educ) {
        String result = "";
        if (educ == null) {
            result = null;
        } else if (educ.equals("1")) {
            result = "81"; // 小学
        } else if (educ.equals("2")) {
            result = "71"; // 初中
        } else if (educ.equals("3")) {
            result = "61"; // 高中/中专
        } else if (educ.equals("4")) {
            result = "31"; // 专科
        } else if (educ.equals("5")) {
            result = "21"; // 本科
        } else if (educ.equals("6")) {
            result = "14"; // 硕士研究生
        } else if (educ.equals("7")) {
            result = "11"; // 博士研究生
        } else if (educ.equals("9")) {
            result = "90"; // 其他
        }
        return result;
    }

    /**
     * 政治面貌
     */
    public static String polctlSt(String st) {
        String polctlSt = "";
        if (st == null) {
            polctlSt = null;
        } else if (st.equals("1")) {
            polctlSt = "01"; // 党员
        } else if (st.equals("2")) {
            polctlSt = "03"; // 团员
        } else if (st.equals("3")) {
            polctlSt = "13"; // 群众
        } else if (st.equals("4")) {
            polctlSt = "05"; // 民主党派
        } else if (st.equals("9")) {
            polctlSt = "99";// 其他
        }
        return polctlSt;
    }

    /**
     * 婚姻状况
     */
    public static String maritalSt(String maritalSt) {
        String result = "";
        if (maritalSt == null) {
            result = null;
        } else if (maritalSt.equals("1")) {
            result = "10";// 未婚
        } else if (maritalSt.equals("2")) {
            result = "20";// 已婚
        } else if (maritalSt.equals("3")) {
            result = "40";// 离异
        } else if (maritalSt.equals("4")) {
            result = "30";// 丧偶
        } else if (maritalSt.equals("9")) {
            result = "50";// 未说明的婚姻状况
        }
        return result;
    }

    /**
     * 服兵役情况
     */
    public static Map<String, String> militarySvcSt() {
        Map<String, String> map = new HashMap<>();
        map.put("NO", "0"); // 未服兵役
        map.put("ON", "4"); // 服现役
        map.put("RE", "2"); // 预备役
        map.put("OU", "1"); // 退出现役
        return map;
    }

    /**
     * 性别
     */
    public static String personGndr(String gndr) {
        String result = "";
        if (gndr == null) {
            result = null;
        } else if (gndr.equals("1")) {
            result = "1"; //男
        } else if (gndr.equals("0")) {
            result = "2";//女
        }
        return result;
    }

    /**
     * 民族
     */
    public static String race(String race) {
        String result = "";
        if (race == null) {
            result = null;
        } else if (race.contains("汉")) {
            result = "01";
        } else if (race.contains("蒙古")) {
            result = "02";
        } else if (race.contains("回")) {
            result = "03";
        } else if (race.contains("藏")) {
            result = "04";
        } else if (race.contains("维吾尔")) {
            result = "05";
        } else if (race.contains("苗")) {
            result = "06";
        } else if (race.contains("彝")) {
            result = "07";
        } else if (race.contains("壮")) {
            result = "08";
        } else if (race.contains("布依")) {
            result = "09";
        } else if (race.contains("朝鲜")) {
            result = "10";
        } else if (race.contains("满")) {
            result = "11";
        } else if (race.contains("侗")) {
            result = "12";
        } else if (race.contains("瑶")) {
            result = "13";
        } else if (race.contains("白")) {
            result = "14";
        } else if (race.contains("土家")) {
            result = "15";
        } else if (race.contains("哈尼")) {
            result = "16";
        } else if (race.contains("哈萨克")) {
            result = "17";
        } else if (race.contains("傣")) {
            result = "18";
        } else if (race.contains("黎")) {
            result = "19";
        } else if (race.contains("傈僳")) {
            result = "20";
        } else if (race.contains("佤")) {
            result = "21";
        } else if (race.contains("畲")) {
            result = "22";
        } else if (race.contains("高山")) {
            result = "23";
        } else if (race.contains("拉祜")) {
            result = "24";
        } else if (race.contains("水")) {
            result = "25";
        } else if (race.contains("东乡")) {
            result = "26";
        } else if (race.contains("纳西")) {
            result = "27";
        } else if (race.contains("景颇")) {
            result = "28";
        } else if (race.contains("柯尔克孜")) {
            result = "29";
        } else if (race.contains("土")) {
            result = "30";
        } else if (race.contains("达翰尔")) {
            result = "31";
        } else if (race.contains("仫佬")) {
            result = "32";
        } else if (race.contains("羌")) {
            result = "33";
        } else if (race.contains("布朗")) {
            result = "34";
        } else if (race.contains("撒拉")) {
            result = "35";
        } else if (race.contains("毛难")) {
            result = "36";
        } else if (race.contains("仡佬")) {
            result = "37";
        } else if (race.contains("锡伯")) {
            result = "38";
        } else if (race.contains("阿昌")) {
            result = "39";
        } else if (race.contains("普米")) {
            result = "40";
        } else if (race.contains("塔吉克")) {
            result = "41";
        } else if (race.contains("怒")) {
            result = "42";
        } else if (race.contains("乌孜别克")) {
            result = "43";
        } else if (race.contains("俄罗斯")) {
            result = "44";
        } else if (race.contains("鄂温克")) {
            result = "45";
        } else if (race.contains("崩龙")) {
            result = "46";
        } else if (race.contains("保安")) {
            result = "47";
        } else if (race.contains("裕固")) {
            result = "48";
        } else if (race.contains("京")) {
            result = "49";
        } else if (race.contains("塔塔尔")) {
            result = "50";
        } else if (race.contains("独龙")) {
            result = "51";
        } else if (race.contains("鄂伦春")) {
            result = "52";
        } else if (race.contains("赫哲")) {
            result = "53";
        } else if (race.contains("门巴")) {
            result = "54";
        } else if (race.contains("珞巴")) {
            result = "55";
        } else if (race.contains("基诺")) {
            result = "56";
        } else {
            result = "97"; // 其他
        }
        return result;
    }

    /**
     * 人口管理类别代码
     */
    public static Map<String, String> popltnMgmtTypeCd() {
        Map<String, String> map = new HashMap<>();
        map.put("Z1", "111"); //居民身份证
        map.put("Z2", "112"); //临时居民身份证
        map.put("Z3", "113"); //户口簿
        map.put("Z4", "114"); //中国人民解放军军官证
        map.put("Z5", "115"); //中国人民武警警察部队警官证
        map.put("Z6", "116"); //暂住证
        map.put("Z7", "133"); //学生证
        map.put("Z8", "151"); //出入证
        map.put("Z9", "211"); //离休证
        map.put("ZA", "117"); //残疾证
        map.put("ZB", "115"); //老年证
        map.put("ZC", "420"); //香港特别行政区护照
        map.put("ZD", "111"); //澳门特别行政区护照
        map.put("ZE", "111"); //台湾居民来往大陆通行证
        map.put("ZF", "111"); //普通护照
        map.put("ZG", "111"); //台湾居民定居证
        map.put("ZH", "111"); //华侨回国定居证
        return map;
    }

    /**
     * 残疾类型
     */
    public static String disableTyp(String disableTyp) {
        String typ = "";
        int cnt = 0;
        if(disableTyp == null){
            return  null;
        }

        if (disableTyp.contains("肢体")) {
            typ = "1";
            cnt++;
        } else if (disableTyp.contains("言语")) {
            typ = "2";
            cnt++;
        } else if (disableTyp.contains("精神")) {
            typ = "3";
            cnt++;
        } else if (disableTyp.contains("智力")) {
            typ = "4";
            cnt++;
        } else if (disableTyp.contains("聋哑")) {
            typ = "5";
            cnt++;
        } else if (disableTyp.contains("视力")) {
            typ = "7";
            cnt++;
        } else if (disableTyp.contains("多重")) {
            typ = "6";
            cnt++;
        } else {
            typ = "99";
        }
        if(cnt > 1){
            typ = "6";
        }
        return typ;
    }

    /**
     * 残疾等级
     */
    public static String disableLvl(String disableLvl) {
        String lvl = "";
        if (disableLvl == null) {
            lvl = null;
        } else if (disableLvl.contains("一") || disableLvl.contains("1")) {
            lvl = "1";
        } else if (lvl.equals("")  && (disableLvl.contains("二") || disableLvl.contains("2"))) {
            lvl = "2";
        } else if (lvl.equals("")  && (disableLvl.contains("三") || disableLvl.contains("3"))) {
            lvl = "3";
        } else if (lvl.equals("")  && (disableLvl.contains("四") || disableLvl.contains("4"))) {
            lvl = "4";
        }
        return lvl;
    }

    /**
     * 证件种类
     */
    public static String crdTyp(String crdTyp){

        if(crdTyp == null){
            return null;
        }
        String result = "";
        if(crdTyp.contains("台胞")){
            result="636"; // 台湾居民登陆证
        }else if(crdTyp.contains("护照")){
            result="414"; // 普通护照
        }
        return result;
    }

    /**
     * 矫正类别
     */
    public static String crrctTyp(String crrctTyp){
        String result = "";
        if(crrctTyp == null){
            return null;
        }
        if(crrctTyp.contains("管制")){
            result = "01";
        }else if(crrctTyp.contains("缓刑")){
            result = "02";
        }else if(crrctTyp.contains("假释")){
            result = "03";
        }else if(crrctTyp.contains("监外")){
            result = "04";
        }else if(crrctTyp.contains("剥夺政治")){
            result = "05";
        }
        return result;
    }
}


