package com.kdc.dataclean.job.Unit;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class DataCleanUnit {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 数据插入到目标表
     * @param table
     * @param updateDataList
     * @param fieldList
     */
    public static void insert(String table, List<Map<String, Object>> updateDataList, List<String> fieldList, JdbcTemplate jdbcTemplate) {
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

    /**
     * 数据更新到目标表
     * @param table
     * @param updateDataList
     * @param fieldList
     */
    public static void update(String table, List<Map<String, Object>> updateDataList, List<String> fieldList, JdbcTemplate jdbcTemplate) {
        String sql = "update " + table + " set " ;
        for (int i = 0; i < fieldList.size(); i++) {
           if(i != fieldList.size()-1){
               sql += fieldList.get(i) + " = ?,";
           }else{
               sql += fieldList.get(i) + " = ?";
           }
        }
        sql += " where iden_crd_num = ?";

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                Map<String, Object> updateData = updateDataList.get(i);
                String idenCrdNum = (String) updateData.get("iden_crd_num");

                for (int j = 0; j < fieldList.size(); j++) {
                    preparedStatement.setObject(j + 1, updateData.get(fieldList.get(j)));
                }
                preparedStatement.setObject(fieldList.size()+1,idenCrdNum);
            }
            @Override
            public int getBatchSize() {
                return updateDataList.size();
            }
        });
    }



    /**
     * 得到清洗状态表的状态code
     * @param id
     * @param tempTableName
     * @param jdbcTemplate
     * @return
     */
    public static String getStCd(Integer id,String tempTableName,JdbcTemplate jdbcTemplate){

        String stCd = null;
        String sql = "select st_cd from new_data_clean_st where temp_table_nam = '" + tempTableName +"'" + " and id = '" + id +"'";

        List<Map<String,Object>> stMapList = jdbcTemplate.queryForList(sql);
        if(stMapList !=null && stMapList.size() > 0){
            stCd = (String) stMapList.get(0).get("st_cd");
        }
        return stCd;
    }

    /**
     * 获取当前时间，格式(yyyy-MM-dd hh:mm:ss)
     */
    public static String getCurrentTime(){
        return sdf.format(new Date());
    }

    /**
     * 获取数据来源sql
     */
    public static  String getSrcSql(String tempTableName,String tempCrdNum){
        String sql = "select a.* from " + tempTableName + " a join\n" +
                "(select trim(" + tempCrdNum +") as " + tempCrdNum + ",max(id)as id from " + tempTableName + " group by "+ "trim(" + tempCrdNum +"))b\n" +
                "on a.id = b.id\n" +
                "and trim(a." + tempCrdNum + ")= trim(b." + tempCrdNum + ")\n" +
                "where a.id not in\n" +
                "(select distinct data_clean_id from new_data_clean_st\n" +
                "where temp_table_nam = '" + tempTableName + "'\n" +
                "and st_cd != 'S')\n" +
                "and a." + tempCrdNum + " is not null\n" +
                "UNION all\n" +
                "select * from " + tempTableName +" where " + tempCrdNum + " is null\n" +
                "order by id";
        return sql;
    }

    /**
     * 获取目标表里面的身份证list
     */

    public static List<String> getIdenList(String destTableName,JdbcTemplate jdbcTemplate) {
        String sql = "select iden_crd_num from " + destTableName ;
        List<String> idenList = jdbcTemplate.queryForList(sql,String.class);
        return idenList;
    }
}
