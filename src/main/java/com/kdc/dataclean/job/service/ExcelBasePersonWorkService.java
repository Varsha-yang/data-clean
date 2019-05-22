package com.kdc.dataclean.job.service;

import com.kdc.dataclean.job.Unit.DataCkeck;
import com.kdc.dataclean.job.Unit.DataCleanUnit;
import com.kdc.dataclean.job.Unit.ExcelDataConvertMap;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ExcelBasePersonWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "base_reg_person_new";
    // 目标表
    private static final String destTableName = "new_base_person";
    // job Id
    private static final String jobId = "ExcelBasePerson";

    // 身份证为空的数量
    int notHandle = 0;
    // check 有问题的数量
    int chkErr = 0;
    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {
        String tempCrdNum = "iden_num";
        // 获取未处理的数据
        String sql = DataCleanUnit.getSrcSql(tempTableName, tempCrdNum);
        List<Map<String, Object>> srcMapList = jdbcTemplate.queryForList(sql);

        // 获取目标表里面的身份证
        List<String> idenList = DataCleanUnit.getIdenList(destTableName, jdbcTemplate);

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
                        String idenNum = ""; // 身份证号
                        String iden = MapUtils.getString(recode, "iden_num");
                        if (iden == null || iden.equals("")) {
                            idenNum = null;
                        } else {
                            idenNum = iden.trim();
                        }
                        String nam = MapUtils.getString(recode, "nam"); // 姓名
                        String gndr = ExcelDataConvertMap.personGndr(MapUtils.getString(recode, "gndr")); // 性别代码
                        String race = ExcelDataConvertMap.race(MapUtils.getString(recode, "race")); // 民族
                        String educDeg = ExcelDataConvertMap.educDeg(MapUtils.getString(recode, "educ")); // 文化程度
                        String maritalSt = ExcelDataConvertMap.maritalSt(MapUtils.getString(recode, "marital_st")); // 婚姻状况
                        String cntctNum = MapUtils.getString(recode, "cntct_num"); // 联系电话
                        if(cntctNum !=null && cntctNum.length()>18){
                            cntctNum = cntctNum.substring(0,18);
                        }

                        String wrkUnit = MapUtils.getString(recode, "wrk_unit"); // 工作单位

                        // 对身份证进行check
                        String reason = "";
                        String status;
                        if (idenNum == null || idenNum.equals("")) {
                            reason = reason + "身份证号码为空!  ";
                            // 失败
                            status = "F";

                            notHandle++;
                        } else {
                            // 对数据进行check
                            Map<String, String> map = DataCkeck.newBasePerson(idenNum);
                            reason = MapUtils.getString(map, "reason");

                            if (!reason.equals("")) {
                                chkErr++;
                                status = "F";
                            } else {
                                status = "S";
                            }
                            if (!status.equals("F")) {
                                String year = idenNum.substring(6,10);
                                String month = idenNum.substring(10,12);
                                String day = idenNum.substring(12,14);
                                String birthDt =  year + "-" + month + "-" + day;// 出生日期

                                if (idenList.contains(idenNum)) {
                                    reason = "身份证号码在" + destTableName + "已存在";
                                    // 不完全成功
                                    status = "N";
                                    version++;
                                    // 数据更新到目标表

                                    Map<String, Object> destUpdateMap = new HashMap<>();
                                    destUpdateMap.put("popltn_mgmt_type_cd", "11"); //本地户籍人口
                                    destUpdateMap.put("iden_crd_num", idenNum);
                                    destUpdateMap.put("nam", nam);
                                    destUpdateMap.put("sex_disct_cd", gndr);
                                    destUpdateMap.put("birth_dt", birthDt);
                                    destUpdateMap.put("ntly_cd", race);
                                    destUpdateMap.put("educ_cd", educDeg);
                                    destUpdateMap.put("marital_st_cd", maritalSt);
                                    destUpdateMap.put("cntct_num", cntctNum);
                                    destUpdateMap.put("svc_loc", wrkUnit);
                                    destUpdateMap.put("created_time", currentTime);
                                    destUpdateMap.put("created_by", jobId);
                                    destUpdateMap.put("updated_by", jobId);
                                    destUpdateMap.put("updated_time", currentTime);
                                    destUpdateMap.put("version", version);
                                    updateDestList.add(destUpdateMap);
                                } else {
                                    // 数据插入到目标表
                                    Map<String, Object> destInsertMap = new HashMap<>();
                                    destInsertMap.put("popltn_mgmt_type_cd", "11"); //本地户籍人口
                                    destInsertMap.put("iden_crd_num", idenNum);
                                    destInsertMap.put("nam", nam);
                                    destInsertMap.put("sex_disct_cd", gndr);
                                    destInsertMap.put("birth_dt", birthDt);
                                    destInsertMap.put("ntly_cd", race);
                                    destInsertMap.put("educ_cd", educDeg);
                                    destInsertMap.put("marital_st_cd", maritalSt);
                                    destInsertMap.put("cntct_num", cntctNum);
                                    destInsertMap.put("svc_loc", wrkUnit);
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
            List<String> destFieldList = Arrays.asList("popltn_mgmt_type_cd","iden_crd_num", "nam", "sex_disct_cd", "birth_dt", "ntly_cd", "educ_cd", "marital_st_cd", "cntct_num", "svc_loc", "created_time", "created_by", "updated_by", "updated_time", "version");

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
        }
    }
}
