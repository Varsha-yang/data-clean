package com.kdc.dataclean.job.service;

import com.kdc.dataclean.job.Unit.CommunityDataConvertMap;
import com.kdc.dataclean.job.Unit.DataCkeck;
import com.kdc.dataclean.job.Unit.DataCleanUnit;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class CommunityBasePersonWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "base_dnt_rsdnt";
    // 目标表
    private static final String destTableName = "new_base_person";
    // job Id
    private static final String jobId = "CommunityBasePerson";

    // 身份证为空的数量
    int notHandle = 0;
    // check 有问题的数量
    int chkErr = 0;

    int gndrErrCnt = 0;
    int raceErrCnt = 0;
    int educErrCnt = 0;
    int polctlStErrCnt = 0;
    int maritalStErrCnt = 0;
    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {
        String tempCrdNum = "crd_num";
        // 获取未处理的数据
        String sql = DataCleanUnit.getSrcSql(tempTableName,tempCrdNum);
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
                        String crdNum = ""; // 身份证号
                        String iden = MapUtils.getString(recode, "crd_num");
                        if(iden == null || iden.equals("")){
                            crdNum = null;
                        }else{
                            crdNum = iden.trim();
                        }
                        String nam = MapUtils.getString(recode, "nam"); // 姓名
                        String gndr = CommunityDataConvertMap.personGndr(MapUtils.getString(recode, "gndr")); // 性别代码
                        String race = CommunityDataConvertMap.race(MapUtils.getString(recode,"race")); // 民族
                        String educ = CommunityDataConvertMap.educDeg(MapUtils.getString(recode, "educ")); // 学历
                        String polctlSt = CommunityDataConvertMap.polctlSt(MapUtils.getString(recode, "polctl_st")); // 政治面貌
                        String maritalSt = CommunityDataConvertMap.maritalSt(MapUtils.getString(recode, "marital_st")); // 婚姻状态
                        String cntctNum = MapUtils.getString(recode, "hp") == null ? MapUtils.getString(recode, "tel") : MapUtils.getString(recode, "hp");// 联系电话
                        if(cntctNum!= null && cntctNum.length()>18){
                            cntctNum = cntctNum.substring(0,18);
                        }
                        if(gndr != null && gndr.equals("")){
                            gndrErrCnt ++;
                        }
                        if(race != null && race.equals("97")){
                            raceErrCnt ++;
                        }
                        if(educ != null && educ.equals("")){
                            educErrCnt ++;
                        }
                        if(polctlSt != null && polctlSt.equals("")){
                            polctlStErrCnt ++;
                        }
                        if(maritalSt != null && maritalSt.equals("")){
                            maritalStErrCnt ++;
                        }
                        String wrkUnit = MapUtils.getString(recode, "wrk_unit"); // 工作单位
                        String ofcTel = MapUtils.getString(recode, "ofc_tel"); // 办公电话

                        // 对身份证进行check
                        String reason = "";
                        String status;
                        if (crdNum == null || crdNum.equals("")) {
                            reason = reason + "身份证号码为空!  ";
                            // 失败
                            status = "F";

                            notHandle++;
                        } else {
                            // 对数据进行check
                            Map<String,String> map = DataCkeck.newBasePerson(crdNum);
                            reason = MapUtils.getString(map,"reason");

                            if (!reason.equals("")) {
                                chkErr++;
                                status = "F";
                            }else{
                                status = "S";
                            }
                            if (!status.equals("F")) {

                                String year = crdNum.substring(6,10);
                                String month = crdNum.substring(10,12);
                                String day = crdNum.substring(12,14);
                                String birthDt =  year + "-" + month + "-" + day;// 出生日期

                                if (idenList.contains(crdNum)) {
                                    reason = "身份证号码在" + destTableName + "已存在";
                                    // 不完全成功
                                    status = "N";
                                    version ++;
                                    // 数据更新到目标表
                                    Map<String, Object> destUpdateMap = new HashMap<>();
                                    destUpdateMap.put("iden_crd_num", crdNum);
                                    destUpdateMap.put("nam", nam);
                                    destUpdateMap.put("sex_disct_cd", gndr);
                                    destUpdateMap.put("birth_dt", birthDt);
                                    destUpdateMap.put("ntly_cd", race);
                                    destUpdateMap.put("educ_cd", educ);
                                    destUpdateMap.put("polctl_st_cd", polctlSt);
                                    destUpdateMap.put("marital_st_cd", maritalSt);
                                    destUpdateMap.put("cntct_num", cntctNum);
                                    destUpdateMap.put("svc_loc", wrkUnit);
                                    destUpdateMap.put("svc_loc_cntct_num", ofcTel);
                                    destUpdateMap.put("created_time", currentTime);
                                    destUpdateMap.put("created_by", jobId);
                                    destUpdateMap.put("updated_by", jobId);
                                    destUpdateMap.put("updated_time", currentTime);
                                    destUpdateMap.put("version", version);
                                    updateDestList.add(destUpdateMap);
                                } else {
                                    // 数据插入到目标表
                                    Map<String, Object> destInsertMap = new HashMap<>();
                                    destInsertMap.put("iden_crd_num", crdNum);
                                    destInsertMap.put("nam", nam);
                                    destInsertMap.put("sex_disct_cd", gndr);
                                    destInsertMap.put("birth_dt", birthDt);
                                    destInsertMap.put("ntly_cd", race);
                                    destInsertMap.put("educ_cd", educ);
                                    destInsertMap.put("polctl_st_cd", polctlSt);
                                    destInsertMap.put("marital_st_cd", maritalSt);
                                    destInsertMap.put("cntct_num", cntctNum);
                                    destInsertMap.put("svc_loc", wrkUnit);
                                    destInsertMap.put("svc_loc_cntct_num", ofcTel);
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
            List<String> destFieldList = Arrays.asList("iden_crd_num", "nam", "sex_disct_cd", "birth_dt", "ntly_cd","educ_cd", "polctl_st_cd","marital_st_cd", "cntct_num", "svc_loc", "svc_loc_cntct_num",  "created_time", "created_by", "updated_by", "updated_time", "version");
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
            if(gndrErrCnt >0){
                logger.info(gndrErrCnt + "条数据性别代码有问题");
            }
            if(raceErrCnt >0){
                logger.info(raceErrCnt + "条数据民族是其他");
            }
            if(educErrCnt >0){
                logger.info(educErrCnt + "条数据学历有问题");
            }
            if(polctlStErrCnt >0){
                logger.info(gndrErrCnt + "条数据政治面貌有问题");
            }
            if(maritalStErrCnt >0){
                logger.info(maritalStErrCnt + "条数据婚姻状态有问题");
            }
        }
    }
}
