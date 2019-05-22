package com.kdc.dataclean.job.service;

import com.kdc.dataclean.job.Unit.CommunityDataConvertMap;
import com.kdc.dataclean.job.Unit.DataCleanUnit;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class CommunityOvrseaPersonWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate2;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "base_dnt_ovrsea";
    // 目标表
    private static final String destTableName = "new_base_ovrsea_person";
    // job Id
    private static final String jobId = "CommunityOvrseaPerson";

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    // 身份证为空的数量
    int notHandle = 0;
    // check 有问题的数量
    int chkErr = 0;
    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {

        String sql = "SELECT\n" +
                "\ta.* \n" +
                "FROM\n" +
                "\tbase_dnt_ovrsea a \n" +
                "WHERE\n" +
                "\ta.id NOT IN ( SELECT DISTINCT data_clean_id FROM new_data_clean_st WHERE temp_table_nam = 'base_dnt_ovrsea' AND st_cd = 'S' )";


        List<Map<String, Object>> srcMapList = jdbcTemplate.queryForList(sql);

        if (srcMapList != null && srcMapList.size() > 0) {
            // 插入目标表的数据
            List<Map<String, Object>> insertDesttList = new ArrayList<>();

            // 插入状态表的数据
            List<Map<String, Object>> stList = new ArrayList<>();

            // 循环list
            srcMapList.forEach(
                    recode -> {
                        String reason = "";
                        String status = "S";
                        String idenNum = ""; // 身份证号
                        String birthDt = null;
                        String iden = MapUtils.getString(recode, "iden_crd_num");
                        if (iden == null || iden.equals("")) {
                            idenNum = null;
                        } else {
                            idenNum = iden.trim();
                            String year = idenNum.substring(6, 10);
                            String month = idenNum.substring(10, 12);
                            String day = idenNum.substring(12, 14);
                            birthDt = year + "-" + month + "-" + day;// 出生日期
                        }
                        Integer id = MapUtils.getInteger(recode,"id");
                        String nam = MapUtils.getString(recode, "nam"); // 姓名
                        String crdTyp = CommunityDataConvertMap.crdTyp(MapUtils.getString(recode, "crd_typ")); //证件种类 todo
                        String crdNum = MapUtils.getString(recode, "crd_num");


                        if (!status.equals("F")) {
                            // 数据插入到目标表
                            Map<String, Object> destInsertMap = new HashMap<>();
                            destInsertMap.put("iden_doc_typ", crdTyp);
                            destInsertMap.put("iden_doc_num", crdNum);
                            destInsertMap.put("cn_nam", nam);
                            destInsertMap.put("birth_dt",birthDt);
                            destInsertMap.put("cntct_iden_crd_num", idenNum);
                            destInsertMap.put("created_time", currentTime);
                            destInsertMap.put("created_by", jobId);
                            destInsertMap.put("updated_by", jobId);
                            destInsertMap.put("updated_time", currentTime);
                            insertDesttList.add(destInsertMap);
                        }

                        Map<String, Object> stMap = new HashMap<>();
                        stMap.put("temp_table_nam", tempTableName);
                        stMap.put("data_clean_id", id);
                        stMap.put("clean_job_id", jobId);
                        stMap.put("st_cd", status);
                        stMap.put("rsn", reason);
                        stMap.put("created_time", currentTime);
                        stList.add(stMap);
                    });

            logger.info("共有数据" + srcMapList.size() + "条");
            // 数据插入到目标表
            List<String> destFieldList = Arrays.asList("iden_doc_typ", "iden_doc_num", "cn_nam", "birth_dt","cntct_iden_crd_num", "created_time", "created_by", "updated_by", "updated_time", "version");
            if (insertDesttList != null && insertDesttList.size() > 0) {
                DataCleanUnit.insert(destTableName, insertDesttList, destFieldList, jdbcTemplate);
            }
            logger.info(insertDesttList.size() + "条数据插入目标表成功");
            // 数据插入到清洗状态表
            List<String> stFieldList = Arrays.asList("temp_table_nam", "data_clean_id", "clean_job_id", "st_cd", "rsn", "created_time");
            if(stList != null && stList.size() > 0){
                DataCleanUnit.insert("new_data_clean_st", stList, stFieldList, jdbcTemplate);
            }
            logger.info(stList.size() + "条数据插入状态表成功");
            logger.info(notHandle + "条数据身份证为空没有处理");
            logger.info(chkErr + "条数据check出来了error");
        }
    }
}
