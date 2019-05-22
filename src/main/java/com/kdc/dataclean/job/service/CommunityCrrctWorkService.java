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
public class CommunityCrrctWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "base_dnt_rectify";
    // 目标表
    private static final String destTableName = "new_base_per_cmmnty_crrct";
    // job Id
    private static final String jobId = "CommunityCrrct";

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    // 身份证为空的数量
    int notHandle = 0;
    // check 有问题的数量
    int chkErr = 0;

    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {
        // 获取基本人口信息的身份证信息
        List<String> basePersonIdenList = DataCleanUnit.getIdenList("new_base_person", jdbcTemplate);
        List<Map<String, Object>> insertBasePersonList = new ArrayList<>();

        String tempCrdNum = "crd_num";
        // 获取未处理的数据
        String sql = DataCleanUnit.getSrcSql(tempTableName, tempCrdNum);
        List<Map<String, Object>> srcMapList = jdbcTemplate.queryForList(sql);

        // 获取目标表里面的身份证
        List<String> idenList = DataCleanUnit.getIdenList(destTableName,jdbcTemplate);

        if (srcMapList != null && srcMapList.size() > 0) {
            // 插入目标表的数据
            List<Map<String, Object>> insertDesttList = new ArrayList<>();

            // 更新目标表的数据
            List<Map<String, Object>> updateDestList = new ArrayList<>();

            // 插入状态表的数据
            List<Map<String, Object>> stList = new ArrayList<>();

            // 循环list
            srcMapList.forEach(
                    recode -> {
                        // 版本
                        Integer version = 1;
                        Integer id = MapUtils.getIntValue(recode, "id");
                        String reason = "";
                        String status = "S";
                        String idenNum = ""; // 身份证号
                        String iden = MapUtils.getString(recode, "crd_num");
                        if (iden == null || iden.equals("")) {
                            idenNum = null;
                        } else {
                            idenNum = iden.trim();
                        }
                        String nam = MapUtils.getString(recode, "nam"); // 姓名
                        String rectifyTyp = CommunityDataConvertMap.crrctTyp(MapUtils.getString(recode, "rectify_typ")); // 矫正类别
                        String relDt = MapUtils.getString(recode, "rel_dt"); // 释放日期
                        String crrctStrDt = MapUtils.getString(recode, "rectify_dt"); // 矫正开始日期

                        if (idenNum == null || idenNum.equals("")) {
                            reason = "身份证号码为空!";
                            // 失败
                            status = "F";
                            notHandle++;
                        } else {
                            // 对数据进行check
                            if (idenNum.length() != 18) {
                                reason = reason + "身份证号码长度不是18位 ";
                                // 失败
                                status = "F";
                            }

                            // 插入基本人口表
                            if (!status.equals("F") && !basePersonIdenList.contains(idenNum)) {
                                String year = idenNum.substring(6, 10);
                                String month = idenNum.substring(10, 12);
                                String day = idenNum.substring(12, 14);
                                String birthDt = year + "-" + month + "-" + day;// 出生日期
                                Map<String, Object> map = new HashMap<>();
                                map.put("iden_crd_num", idenNum);
                                map.put("nam", nam);
                                map.put("birth_dt", birthDt);
                                map.put("created_time", currentTime);
                                map.put("created_by", jobId);
                                map.put("updated_by", jobId);
                                map.put("updated_time", currentTime);
                                map.put("version", version);
                                insertBasePersonList.add(map);
                            }

                            if (status.equals("F")) {
                                chkErr++;
                            }

                            if (!status.equals("F")) {
                                if (idenList.contains(idenNum)) {
                                    reason = "身份证号码在" + destTableName + "已存在";
                                    // 不完全成功
                                    status = "N";
                                    version++;
                                    // 数据更新到目标表
                                    Map<String, Object> destUpdateMap = new HashMap<>();
                                    destUpdateMap.put("iden_crd_num", idenNum);
                                    destUpdateMap.put("nam", nam);
                                    destUpdateMap.put("crrct_typ_cd", rectifyTyp);
                                    destUpdateMap.put("orig_sentnc_end_dt", relDt);
                                    destUpdateMap.put("crrct_str_dt", crrctStrDt);
                                    destUpdateMap.put("created_time", currentTime);
                                    destUpdateMap.put("created_by", jobId);
                                    destUpdateMap.put("updated_by", jobId);
                                    destUpdateMap.put("updated_time", currentTime);
                                    destUpdateMap.put("version", version);
                                    updateDestList.add(destUpdateMap);

                                } else {
                                    // 数据插入到目标表
                                    Map<String, Object> destInsertMap = new HashMap<>();
                                    destInsertMap.put("iden_crd_num", idenNum);
                                    destInsertMap.put("nam", nam);
                                    destInsertMap.put("crrct_typ_cd", rectifyTyp);
                                    destInsertMap.put("orig_sentnc_end_dt", relDt);
                                    destInsertMap.put("crrct_str_dt", crrctStrDt);
                                    destInsertMap.put("created_time", currentTime);
                                    destInsertMap.put("created_by", jobId);
                                    destInsertMap.put("updated_by", jobId);
                                    destInsertMap.put("updated_time", currentTime);
                                    destInsertMap.put("version", version);
                                    insertDesttList.add(destInsertMap);
                                }
                            }

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
            List<String> destFieldList = Arrays.asList("iden_crd_num", "nam", "crrct_typ_cd", "orig_sentnc_end_dt", "crrct_str_dt","created_time", "created_by", "updated_by", "updated_time", "version");
            if (insertDesttList != null && insertDesttList.size() > 0) {
                DataCleanUnit.insert(destTableName, insertDesttList, destFieldList, jdbcTemplate);
            }
            logger.info(insertDesttList.size() + "条数据插入目标表成功");
            // 数据更新到目标表
            if (updateDestList != null && updateDestList.size() > 0) {
                DataCleanUnit.update(destTableName, updateDestList, destFieldList, jdbcTemplate);
            }
            logger.info(updateDestList.size() + "条数据更新目标表成功");
            // 数据插入到清洗状态表
            List<String> stFieldList = Arrays.asList("temp_table_nam", "data_clean_id", "clean_job_id", "st_cd", "rsn", "created_time");
            DataCleanUnit.insert("new_data_clean_st", stList, stFieldList, jdbcTemplate);
            logger.info(stList.size() + "条数据插入状态表成功");
            logger.info(notHandle + "条数据身份证为空没有处理");
            logger.info(chkErr + "条数据check出来了error");


            if (insertBasePersonList != null && insertBasePersonList.size() > 0) {
                List<String> fieldList = Arrays.asList("iden_crd_num", "nam", "birth_dt", "created_time", "created_by", "updated_by", "updated_time", "version");
                DataCleanUnit.insert("new_base_person", insertBasePersonList, fieldList, jdbcTemplate);
            }
            logger.info("插入基本人口表" + insertBasePersonList.size() + "条");

        }
    }
}
