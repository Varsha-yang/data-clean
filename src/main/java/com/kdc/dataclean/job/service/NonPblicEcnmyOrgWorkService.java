package com.kdc.dataclean.job.service;

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
public class NonPblicEcnmyOrgWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate2;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "tbl_coy_cndtn";
    // 目标表
    private static final String destTableName = "new_base_non_pblic_ecnmy_org";
    // job Id
    private static final String jobId = "nonPblicEcnmyOrg";

    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    // 身份证为空的数量
    int notHandle = 0;
    // check 有问题的数量
    int chkErr = 0;
    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData(){

        String sql = "select * from " + tempTableName;
        List<Map<String, Object>> srcMapList = jdbcTemplate2.queryForList(sql);

        if (srcMapList != null && srcMapList.size() > 0) {
            // 插入目标表的数据
            List<Map<String, Object>> insertDesttList = new ArrayList<>();

            // 插入状态表的数据
            List<Map<String, Object>> stList = new ArrayList<>();

            // 循环list
            srcMapList.forEach(
                    recode -> {
                        Integer id = MapUtils.getInteger(recode,"id");
                        String coyNam = MapUtils.getString(recode,"coy_nam"); //企业名称
                        String addr = MapUtils.getString(recode,"addr"); //地址
                        String lkMan = MapUtils.getString(recode,"lk_man"); //联系人
                        String cntct = MapUtils.getString(recode,"cntct"); //联系方式
                        String coyScale = MapUtils.getString(recode,"coy_scale"); //企业规模（小微、规上）
                        String cmplCndtn = MapUtils.getString(recode,"cmpl_cndtn"); //标准化完成情况（一级、二级、三级、小微）
                        String area = MapUtils.getString(recode,"cmpl_cndtn"); //属地
                        String reason = "";
                        String status = "S";
                        int version = 1;
                        // 数据插入到目标表
                        Map<String, Object> destInsertMap = new HashMap<>();
                        destInsertMap.put("coy_nam", coyNam);
                        destInsertMap.put("coy_addr", addr);
                        destInsertMap.put("legal_person", lkMan);
                        destInsertMap.put("coy_cntct_num", cntct);
                        destInsertMap.put("biz_scope", coyScale);
                        destInsertMap.put("created_time", currentTime);
                        destInsertMap.put("created_by", jobId);
                        destInsertMap.put("updated_by", jobId);
                        destInsertMap.put("updated_time", currentTime);
                        destInsertMap.put("version", version);
                        insertDesttList.add(destInsertMap);

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
            List<String> destFieldList = Arrays.asList("coy_nam", "coy_addr", "legal_person", "coy_cntct_num","biz_scope","created_time","created_by","updated_by","updated_time","version");
            if (insertDesttList != null && insertDesttList.size() > 0) {
                DataCleanUnit.insert(destTableName, insertDesttList, destFieldList, jdbcTemplate);
            }
            logger.info(insertDesttList.size() + "条数据插入目标表成功");

            // 数据插入到清洗状态表
            List<String> stFieldList = Arrays.asList("temp_table_nam", "data_clean_id", "clean_job_id", "st_cd", "rsn","created_time");
            DataCleanUnit.insert("new_data_clean_st", stList, stFieldList, jdbcTemplate);
            logger.info(stList.size() + "条数据插入状态表成功");
            logger.info(notHandle + "条数据身份证为空没有处理");
            logger.info(chkErr + "条数据check出来了error");
        }
    }
}
