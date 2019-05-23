package com.kdc.dataclean.job.service;

import com.kdc.dataclean.job.Unit.DataCleanUnit;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class AddRessHseWorkService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate2;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    // 临时表名
    private static final String tempTableName = "tbl_hse_modl";
    // 目标表
    private static final String destTableName = "new_base_addr_hse";
    // job Id
    private static final String jobId = "AddRessHse";

    // 获得当前时间
    String currentTime = DataCleanUnit.getCurrentTime();

    public void cleanData() {

        String sql = "select cmmnty,hse_nam from " + tempTableName + " group by cmmnty,hse_nam";
        List<Map<String, Object>> srcMapList = jdbcTemplate2.queryForList(sql);


        String sql2 = "select a.*,concat(b.govt_admin_area_nam,a.name) as detail_address\n" +
                "from \n" +
                "(SELECT\n" +
                "\ta.cmmnty_cd,\n" +
                "\ta.cmmnty_nam,\n" +
                "\t\n" +
                "\tconcat(b.govt_admin_area_nam,a.NAME) as name,\n" +
                "\tb.parent_govt_admin_area_addr_cd\n" +
                "FROM\n" +
                "\t(\n" +
                "SELECT\n" +
                "\ta.cmmnty_cd,\n" +
                "\tconcat( c.govt_admin_area_nam, b.ts_nam, a.cmmnty_nam ) AS NAME,\n" +
                "\ta.cmmnty_nam,\n" +
                "\tc.parent_govt_admin_area_addr_cd \n" +
                "FROM\n" +
                "\tnew_base_addr_cmmnty a\n" +
                "\tJOIN new_base_addr_ts b ON a.parent_govt_admin_area_addr_cd = b.ts_cd\n" +
                "\tJOIN new_base_addr_city c ON b.parent_govt_admin_area_addr_cd = c.govt_admin_area_cd \n" +
                "\t) a\n" +
                "\tJOIN new_base_addr_city b ON a.parent_govt_admin_area_addr_cd = b.govt_admin_area_cd) a\n" +
                "\tjoin new_base_addr_city b\n" +
                "\ton a.parent_govt_admin_area_addr_cd = b.govt_admin_area_cd";
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql2);
        if (srcMapList != null && srcMapList.size() > 0) {
            // 插入目标表的数据
            List<Map<String, Object>> insertDesttList = new ArrayList<>();
            // 循环list
            srcMapList.forEach(
                    recode -> {
                        Integer version = 1;
                        String cmmntyNam = MapUtils.getString(recode, "cmmnty");
                        String hseNam = MapUtils.getString(recode, "hse_nam");
                        String hseAddrNam = null; // 地址（房屋）名称
                        String prvncCityCtry = null; // 省市县（区）
                        String detlAddr = null; // 区划内详细地址
                        String streetNam = null; //所属街路巷（小区）_地址编码
                        // String min_addr_cd = null; //所属最低一级行政区域_地址编码
                        for (Map<String, Object> map : mapList) {
                            if (MapUtils.getString(map, "cmmnty_nam").equals(cmmntyNam)) {
                                hseAddrNam = hseNam;
                                detlAddr = MapUtils.getString(map,"detail_address")+hseNam;
                                prvncCityCtry = MapUtils.getString(map,"cmmnty_cd").substring(0,6);
                                streetNam = MapUtils.getString(map, "cmmnty_nam");
                            }
                        }

                        // 数据插入到目标表
                        Map<String, Object> destInsertMap = new HashMap<>();
                        destInsertMap.put("addr_elem_class", "60");
                        destInsertMap.put("hse_addr_nam", hseAddrNam);
                        destInsertMap.put("detl_addr", detlAddr);
                        destInsertMap.put("prvnc_city_ctry", prvncCityCtry);
                        destInsertMap.put("street_nam", streetNam);
                        destInsertMap.put("created_time", currentTime);
                        destInsertMap.put("created_by", jobId);
                        destInsertMap.put("updated_by", jobId);
                        destInsertMap.put("updated_time", currentTime);
                        destInsertMap.put("version", version);
                        insertDesttList.add(destInsertMap);
                    });

            logger.info("共有数据" + srcMapList.size() + "条");
            // 数据插入到目标表
            List<String> destFieldList = Arrays.asList("addr_elem_class", "hse_addr_nam", "detl_addr", "prvnc_city_ctry", "street_nam", "created_time", "created_by", "updated_by", "updated_time", "version");
            if (insertDesttList != null && insertDesttList.size() > 0) {
                DataCleanUnit.insert(destTableName, insertDesttList, destFieldList, jdbcTemplate);
            }
            logger.info(insertDesttList.size() + "条数据插入目标表成功");
        }
    }
}

