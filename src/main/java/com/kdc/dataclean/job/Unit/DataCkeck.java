package com.kdc.dataclean.job.Unit;

import java.util.HashMap;
import java.util.Map;

public class DataCkeck {
    /**
     * new_base_person的check
     */
    public static Map<String,String> newBasePerson(String idenCrdNum){
        Map<String,String> map = new HashMap<>();
        String reason = "";

        if (idenCrdNum.length() != 18) {
            reason = reason + "身份证号码长度不是18位 ";
        }
        map.put("reason",reason);
        return map;
    }
}
