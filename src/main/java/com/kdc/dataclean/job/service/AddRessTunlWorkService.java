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
import java.util.stream.Collectors;

@Service
public class AddRessTunlWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate2;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "tbl_hse_modl";
    // 目标表
    private static final String destTableName = "new_base_addr_tunl";
    // job Id
    private static final String jobId = "AddRessTunl";

    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {

        String sql = "select cmmnty,hse_nam from " + tempTableName + " group by cmmnty,hse_nam";
        List<Map<String, Object>> srcMapList = jdbcTemplate2.queryForList(sql);

        if (srcMapList != null && srcMapList.size() > 0) {
            // 插入目标表的数据
            List<Map<String, Object>> insertDesttList = new ArrayList<>();

            List<Map<String, Object>> cmmntyList = jdbcTemplate.queryForList("select * from new_base_addr_cmmnty");

            // 循环list
            srcMapList.forEach(
                    recode -> {
                        Integer version = 1;
                        String cmmntyNam = MapUtils.getString(recode, "cmmnty");
                        String hseNam = MapUtils.getString(recode, "hse_nam");
                        String cmmntyCd = null; // 街路巷代码
                        String streetNam = null; // 街路巷名称
                        String alias = null; // 别名
                        String ownStreetAddrCd = null; //所属街路巷（小区）_地址编码
                        // String min_addr_cd = null; //所属最低一级行政区域_地址编码
                        for (Map<String, Object> map : cmmntyList) {
                            if (MapUtils.getString(map, "cmmnty_nam").equals(cmmntyNam)) {
                                cmmntyCd = MapUtils.getString(map, "cmmnty_cd");
                                streetNam = cmmntyNam;
                                alias = hseNam;
                                ownStreetAddrCd = MapUtils.getString(map, "parent_govt_admin_area_addr_cd");

                            }
                        }

                        // 数据插入到目标表
                        Map<String, Object> destInsertMap = new HashMap<>();
                        destInsertMap.put("addr_elem_class", "42");
                        destInsertMap.put("street_cd", cmmntyCd);
                        destInsertMap.put("street_nam", streetNam);
                        destInsertMap.put("alias", alias);
                        destInsertMap.put("own_street_addr_cd", ownStreetAddrCd);
                        destInsertMap.put("created_time", currentTime);
                        destInsertMap.put("created_by", jobId);
                        destInsertMap.put("updated_by", jobId);
                        destInsertMap.put("updated_time", currentTime);
                        destInsertMap.put("version", version);
                        insertDesttList.add(destInsertMap);
                    });

            logger.info("共有数据" + srcMapList.size() + "条");
            // 数据插入到目标表
            List<String> destFieldList = Arrays.asList("addr_elem_class", "street_cd", "street_nam","alias", "own_street_addr_cd", "created_time", "created_by", "updated_by", "updated_time", "version");
            if (insertDesttList != null && insertDesttList.size() > 0) {
                DataCleanUnit.insert(destTableName, insertDesttList, destFieldList, jdbcTemplate);
            }
            logger.info(insertDesttList.size() + "条数据插入目标表成功");
        }
    }
}

