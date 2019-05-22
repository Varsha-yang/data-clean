package com.kdc.dataclean.job.Unit;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CollectionDataConvertMap {


    /**
     * 文化程度
     */
    public static Map<String, String> educDeg() {
        Map<String, String> map = new HashMap<>();
        map.put("DOC", "11"); // 博士
        map.put("GRA", "11"); // 硕士
        map.put("BAC", "21"); // 本科
        map.put("COL", "31"); // 专科
        map.put("SEC", "41"); // 中专
        map.put("TEC", "47"); // 技工学校
        map.put("SHS", "61"); // 高中
        map.put("JHS", "71"); // 初中
        map.put("PRS", "81"); // 小学
        map.put("ILL", "90"); // 文盲与半文盲
        return map;
    }

    /**
     * 政治面貌
     */
    public static Map<String, String> polctlSt() {
        Map<String, String> map = new HashMap<>();
        map.put("ZD", "01"); // 中国共产党党员
        map.put("ZY", "02"); // 中国共产党预备党员
        map.put("GQ", "03"); // 中国共产主义青年团团员
        map.put("MG", "04"); // 中国国民党革命委员会会员
        map.put("MM", "05"); // 中国民主同盟盟员
        map.put("MJ", "06"); // 中国民主建国会会员
        map.put("MH", "07"); // 中国民主促进会会员
        map.put("NG", "08"); // 中国农工民主党党员
        map.put("ZG", "09"); // 中国致公党党员
        map.put("XS", "10"); // 九三学社社员
        map.put("TM", "11"); // 台湾民主自治同盟盟员
        map.put("WD", "12"); // 无党派民主人士
        map.put("QZ", "13"); // 群众
        map.put("OT", "99"); // 其他
        return map;
    }

    /**
     * 婚姻状况
     */
    public static Map<String, String> maritalSt() {
        Map<String, String> map = new HashMap<>();
        map.put("UM", "10"); // 未婚
        map.put("MA", "20"); // 已婚
        map.put("WI", "30"); // 丧偶
        map.put("DI", "40"); // 离异
        return map;
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
    public static  String personGndr(String gndr) {
        String result = "";
        if(gndr.equals("男") || gndr.equals("1")){
            result = "1";
        }else if(gndr.equals("女") || gndr.equals("0")){
            result = "2";
        }
        return result;
    }

    /**
     * 民族
     */
    public static Map<String, String> race() {
        Map<String, String> map = new HashMap<>();
        map.put("R01", "1"); // 汉族
        map.put("R02", "2"); // 蒙古族
        map.put("R03", "3"); // 回族
        map.put("R04", "4"); // 藏族
        map.put("R05", "5"); // 维吾尔族
        map.put("R06", "6"); // 苗族
        map.put("R07", "7"); // 彝族
        map.put("R08", "8"); // 壮族
        map.put("R09", "9"); // 布依族
        map.put("R10", "10"); // 朝鲜族
        map.put("R11", "11"); // 满族
        map.put("R12", "12"); // 侗族
        map.put("R13", "13"); // 瑶族
        map.put("R14", "14"); // 白族
        map.put("R15", "15"); // 土家族
        map.put("R16", "16"); // 哈尼族
        map.put("R17", "17"); // 哈萨克族
        map.put("R18", "18"); // 傣族
        map.put("R19", "19"); // 黎族
        map.put("R20", "20"); // 傈僳族
        map.put("R21", "21"); // 佤族
        map.put("R22", "22"); // 畲族
        map.put("R23", "23"); // 高山族
        map.put("R24", "24"); // 拉祜族
        map.put("R25", "25"); // 水族
        map.put("R26", "26"); // 东乡族
        map.put("R27", "27"); // 纳西族
        map.put("R28", "28"); // 景颇族
        map.put("R29", "29"); // 柯尔克孜族
        map.put("R30", "30"); // 土族
        map.put("R31", "31"); // 达翰尔族
        map.put("R32", "32"); // 仫佬族
        map.put("R33", "33"); // 羌族
        map.put("R34", "34"); // 布朗族
        map.put("R35", "35"); // 撒拉族
        map.put("R36", "36"); // 毛难族
        map.put("R37", "37"); // 仡佬族
        map.put("R38", "38"); // 锡伯族
        map.put("R39", "39"); // 阿昌族
        map.put("R40", "40"); // 普米族
        map.put("R41", "41"); // 塔吉克族
        map.put("R42", "42"); // 怒族
        map.put("R43", "43"); // 乌孜别克族
        map.put("R44", "44"); // 俄罗斯族
        map.put("R45", "45"); // 鄂温克族
        map.put("R46", "46"); // 崩龙族
        map.put("R47", "47"); // 保安族
        map.put("R48", "48"); // 裕固族
        map.put("R49", "49"); // 京族
        map.put("R50", "50"); // 塔塔尔族
        map.put("R51", "51"); // 独龙族
        map.put("R52", "52"); // 鄂伦春族
        map.put("R53", "53"); // 赫哲族
        map.put("R54", "54"); // 门巴族
        map.put("R55", "55"); // 珞巴族
        map.put("R56", "56"); // 基诺族
        return map;
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
        if(disableTyp == null || disableTyp.equals("")){
            return null;
        }
        String typ = null;
        if(disableTyp.contains("肢体")){
            typ = "1";
        }else if(disableTyp.contains("言语") || disableTyp.contains("语言")){
            typ = "2";
        }else if(disableTyp.contains("精神")){
            typ = "3";
        }else if(disableTyp.contains("智力")){
            typ = "4";
        }else if(disableTyp.contains("聋哑")){
            typ = "5";
        }else if(disableTyp.contains("多重")){
            typ = "6";
        }else if(disableTyp.contains("视力")){
            typ = "7";
        }else {
            typ = "9";
        }
        return typ;
    }

    /**
     * 残疾等级
     */
    public static String disableLvl(String disableLvl) {
        if(disableLvl == null || disableLvl.equals("")){
            return null;
        }else{
            String lvl = null;
            if(disableLvl.contains("一") || disableLvl.contains("1")){
                lvl = "1";
            }else if(disableLvl.contains("二")  || disableLvl.contains("2")){
                lvl = "2";
            }else if(disableLvl.contains("三") || disableLvl.contains("3")){
                lvl = "3";
            }else if(disableLvl.contains("四") || disableLvl.contains("4")){
                lvl = "4";
            }else if(disableLvl.contains("未达标")){
                lvl = "0";
            }else{
                lvl = disableLvl;
            }
            return lvl;
        }

    }
}

