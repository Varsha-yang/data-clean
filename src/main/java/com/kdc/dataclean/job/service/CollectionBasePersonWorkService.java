package com.kdc.dataclean.job.service;

import com.kdc.dataclean.job.Unit.CollectionDataConvertMap;
import com.kdc.dataclean.job.Unit.DataCkeck;
import com.kdc.dataclean.job.Unit.DataCleanUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.apache.commons.collections.MapUtils;

import java.util.*;

@Service
public class CollectionBasePersonWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "base_person";
    // 目标表
    private static final String destTableName = "new_base_person";
    // job Id
    private static final String jobId = "CollectionBasePerson";

    // 身份证为空的数量
    int notHandle = 0;
    // check 有问题的数量
    int chkErr = 0;
    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {
        String tempCrdNum = "iden_crd";

        // 获取未处理的数据
        String sql = DataCleanUnit.getSrcSql(tempTableName, tempCrdNum);
        List<Map<String, Object>> srcMapList = jdbcTemplate.queryForList(sql);

        // 获取目标表里面的身份证
        List<String> idenList = DataCleanUnit.getIdenList(destTableName, jdbcTemplate);

        // 判断是不是Excel导入的
        String excelSql = "select * from kdcs_imp_job_tbl where tbl_nam = 'base_person'";
        List<Map<String, Object>> excelMapList = jdbcTemplate.queryForList(excelSql);
        List<String> jobIdList = new ArrayList<>();
        excelMapList.forEach(excel -> {
            String impJobId = MapUtils.getString(excel, "impl_job_id");
            if (impJobId != null && impJobId != "") {
                jobIdList.add(MapUtils.getString(excel, "job_id"));
            }
        });
        // 更新导入JOB历史表（kdcs_imp_job）
        if (jobIdList != null && jobIdList.size() > 0) {

            for (String jobId : jobIdList) {

                String updateSql = "update kdcs_imp_job set clean_job_st = 'I' where id = jobId";
                jdbcTemplate.update(updateSql);
            }
        }


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
                        String nam = MapUtils.getString(recode, "person_nam"); // 姓名
                        String idenCrdNum; // 身份证号
                        String iden = MapUtils.getString(recode, "iden_crd");
                        if (iden == null || iden.equals("")) {
                            idenCrdNum = null;
                        } else {
                            idenCrdNum = iden.trim();
                        }
                        String fmrNam = MapUtils.getString(recode, "fmr_nam"); // 曾用名
                        String personGndr = MapUtils.getString(recode, "person_gndr"); // 性别代码
                        String educDeg = CollectionDataConvertMap.educDeg().get(MapUtils.getString(recode, "educ_deg")); // 文化程度
                        String natvPlaceId = MapUtils.getString(recode, "natv_place_id"); // 籍贯信息ID todo 没有籍贯信息的对应关系
                        String polctlSt = CollectionDataConvertMap.polctlSt().get(MapUtils.getString(recode, "polctl_st")); // 政治面貌
                        String maritalSt = CollectionDataConvertMap.maritalSt().get(MapUtils.getString(recode, "marital_st")); // 婚姻状况
                        String militarySvcSt = CollectionDataConvertMap.militarySvcSt().get(MapUtils.getString(recode, "military_svc_st")); // 服兵役情况 todo 预备役区分不清楚
                        String dmclPlace = MapUtils.getString(recode, "dmcl_place"); // 户籍地址 todo 国家标准只有100位
                        String cntctNum = MapUtils.getString(recode, "cntct_num"); // 联系电话
                        if (cntctNum != null && cntctNum.length() > 18) {
                            cntctNum = cntctNum.substring(0, 18);
                        }
                        // 对身份证进行check
                        String reason = "";
                        String status;
                        if (idenCrdNum == null || idenCrdNum.equals("")) {
                            reason = reason + "身份证号码为空!  ";
                            // 失败
                            status = "F";

                            notHandle++;
                        } else {
                            // 对数据进行check
                            Map<String, String> map = DataCkeck.newBasePerson(idenCrdNum);
                            reason = MapUtils.getString(map, "reason");

                            if (!reason.equals("")) {
                                chkErr++;
                                status = "F";
                            } else {
                                status = "S";
                            }
                            if (!status.equals("F")) {

                                String year = idenCrdNum.substring(6, 10);
                                String month = idenCrdNum.substring(10, 12);
                                String day = idenCrdNum.substring(12, 14);
                                String birthDt = year + "-" + month + "-" + day;// 出生日期

                                if (idenList.contains(idenCrdNum)) {
                                    reason = reason + "身份证号码在" + destTableName + "已存在";
                                    // 不完全成功
                                    status = "N";
                                    version++;
                                    // 数据更新到目标表
                                    Map<String, Object> destUpdateMap = new HashMap<>();
                                    destUpdateMap.put("nam", nam);
                                    destUpdateMap.put("iden_crd_num", idenCrdNum);
                                    destUpdateMap.put("fmr_nam", fmrNam);
                                    destUpdateMap.put("sex_disct_cd", personGndr);
                                    destUpdateMap.put("birth_dt", birthDt);
                                    destUpdateMap.put("educ_cd", educDeg);
                                    destUpdateMap.put("natv_place_cd", natvPlaceId);
                                    destUpdateMap.put("polctl_st_cd", polctlSt);
                                    destUpdateMap.put("marital_st_cd", maritalSt);
                                    destUpdateMap.put("cndtn_svc_cd", militarySvcSt);
                                    destUpdateMap.put("dmcl_place_detl_addr", dmclPlace);
                                    destUpdateMap.put("cntct_num", cntctNum);
                                    destUpdateMap.put("created_time", currentTime);
                                    destUpdateMap.put("created_by", jobId);
                                    destUpdateMap.put("updated_by", jobId);
                                    destUpdateMap.put("updated_time", currentTime);
                                    destUpdateMap.put("version", version);
                                    updateDestList.add(destUpdateMap);
                                } else {
                                    // 数据插入到目标表
                                    Map<String, Object> destInsertMap = new HashMap<>();
                                    destInsertMap.put("nam", nam);
                                    destInsertMap.put("iden_crd_num", idenCrdNum);
                                    destInsertMap.put("fmr_nam", fmrNam);
                                    destInsertMap.put("sex_disct_cd", personGndr);
                                    destInsertMap.put("birth_dt", birthDt);
                                    destInsertMap.put("educ_cd", educDeg);
                                    destInsertMap.put("natv_place_cd", natvPlaceId);
                                    destInsertMap.put("polctl_st_cd", polctlSt);
                                    destInsertMap.put("marital_st_cd", maritalSt);
                                    destInsertMap.put("cndtn_svc_cd", militarySvcSt);
                                    destInsertMap.put("dmcl_place_detl_addr", dmclPlace);
                                    destInsertMap.put("cntct_num", cntctNum);
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
            List<String> destFieldList = Arrays.asList("nam", "iden_crd_num", "fmr_nam", "sex_disct_cd", "birth_dt",
                    "educ_cd", "natv_place_cd", "polctl_st_cd", "marital_st_cd", "cndtn_svc_cd", "dmcl_place_detl_addr", "cntct_num", "created_time", "created_by", "updated_by", "updated_time", "version");
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

            // 更新导入JOB历史表（kdcs_imp_job）
            if (jobIdList != null && jobIdList.size() > 0) {
                for (String jobId : jobIdList) {
                    String updateSql = "update kdcs_imp_job set clean_job_st = 'C' where id = jobId";
                    jdbcTemplate.update(updateSql);
                }
            }
        }
    }
}

