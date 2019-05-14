package com.kdc.dataclean.job.Unit;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DataCleanUnit {
    /**
     * 数据插入到目标表
     * @param table
     * @param updateDataList
     * @param fieldList
     */
    public static void update(String table, List<Map<String, Object>> updateDataList, List<String> fieldList, JdbcTemplate jdbcTemplate) {
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
