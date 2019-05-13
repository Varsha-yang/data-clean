package com.kdc.dataclean.job.service;

import com.kdc.dataclean.job.config.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

@Service
public class WorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    public void cleanData() throws Exception {

        String sql = "select * from base_person";
        List<Map<String, Object>> srcMapList = jdbcTemplate.queryForList(sql);

        if(srcMapList != null && srcMapList.size() >0 ){
            List<Map<String, Object>> toList = new ArrayList<>();
            // 循环list
            srcMapList.forEach(
                    recode->{
                        String name = MapUtils.getString(recode,"person_nam"); // 姓名
                        String idenCrdNum = MapUtils.getString(recode,"iden_crd"); // 身份证号
                        String fmrNam = MapUtils.getString(recode,"fmr_nam"); // 曾用名
                        String personGndr = MapUtils.getString(recode,"person_gndr"); // 性别代码
                        // 出生日期
                        String date = MapUtils.getString(recode,"birth_dt");
                        Date birthDt = null;
                        try {
                            if(date != null){
                                birthDt = sdf.parse(date);
                            }
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        String educDeg = Unit.educDeg().get(MapUtils.getString(recode,"educ_deg")); // 文化程度 todo 博士和硕士没有分清楚
                        String natvPlaceId = MapUtils.getString(recode,"natv_place_id"); // 籍贯信息ID todo 没有籍贯信息的对应关系
                        String polctlSt = Unit.polctlSt().get(MapUtils.getString(recode,"polctl_st")); // 政治面貌
                        String maritalSt = Unit.maritalSt().get(MapUtils.getString(recode,"marital_st")); // 婚姻状况
                        String militarySvcSt = Unit.militarySvcSt().get(MapUtils.getString(recode,"military_svc_st")); // 服兵役情况 todo 预备役区分不清楚
                        String dmclPlace = MapUtils.getString(recode,"dmcl_place"); // 户籍地址 todo 国家标准只有100位
                        String cntctNum = MapUtils.getString(recode,"cntct_num"); // 联系电话

                        Map<String, Object> recodeMap = new HashMap<>();
                        // 数据映射到目标表
                        recodeMap.put("nam",name);
                        recodeMap.put("iden_crd_num",idenCrdNum);
                        recodeMap.put("fmr_nam",fmrNam);
                        recodeMap.put("sex_disct_cd",personGndr);
                        recodeMap.put("birth_dt",birthDt);
                        recodeMap.put("educ_cd",educDeg);
                        recodeMap.put("natv_place_cd",natvPlaceId);
                        recodeMap.put("polctl_st_cd",polctlSt);
                        recodeMap.put("marital_st_cd",maritalSt);
                        recodeMap.put("cndtn_svc_cd",militarySvcSt);
                        recodeMap.put("dmcl_place_detl_addr",dmclPlace);
                        recodeMap.put("cntct_num",cntctNum);
                        toList.add(recodeMap);
                    }
            );
            List<String> fieldList = Arrays.asList("nam","iden_crd_num","fmr_nam","sex_disct_cd","birth_dt","educ_cd","natv_place_cd","polctl_st_cd","marital_st_cd","cndtn_svc_cd","dmcl_place_detl_addr","cntct_num");
            // 数据插入到目标表
            update("new_base_person",toList,fieldList);
            logger.info("数据插入成功");
        }
    }

    /**
     * 数据插入到目标表
     * @param table
     * @param updateDataList
     * @param fieldList
     */
    private void update(String table, List<Map<String, Object>> updateDataList, List<String> fieldList) {
        String sql = "INSERT INTO " + table + "(" + StringUtils.join(fieldList, ",") + ") values (";
        for (int i = 0; i < fieldList.size(); i++) {
            if (i > 0) {
                sql += ",";
            }
            sql += "?";
        }
        sql += ")";
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                Map<String, Object> updateData = updateDataList.get(i);
                for (int j = 0; j < fieldList.size(); j++) {
                    preparedStatement.setObject(j + 1, updateData.get(fieldList.get(j)));
                }
            }
            @Override
            public int getBatchSize() {
                return updateDataList.size();
            }
        });
    }
}
