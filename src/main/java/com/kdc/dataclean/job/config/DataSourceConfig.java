package com.kdc.dataclean.job.config;
import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;


import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {
    @Value("${spring.datasource.url}")
    private String url;
    @Value("${spring.datasource.type}")
    private String type;
    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    @Value("${spring.datasource.testWhileIdle}")
    private boolean testWhileIdle;
    @Value("${spring.datasource.validationQuery}")
    private String validationQuery;
    @Value("${spring.datasource.testOnBorrow}")
    private boolean testOnBorrow;
    @Value("${spring.datasource.testOnReturn}")
    private boolean testOnReturn;

    @Value("${spring.datasource2.url}")
    private String url2;
    @Value("${spring.datasource2.type}")
    private String type2;
    @Value("${spring.datasource2.driver-class-name}")
    private String driverClassName2;
    @Value("${spring.datasource2.username}")
    private String username2;
    @Value("${spring.datasource2.password}")
    private String password2;
    @Value("${spring.datasource2.testWhileIdle}")
    private boolean testWhileIdle2;
    @Value("${spring.datasource.validationQuery}")
    private String validationQuery2;
    @Value("${spring.datasource2.testOnBorrow}")
    private boolean testOnBorrow2;
    @Value("${spring.datasource2.testOnReturn}")
    private boolean testOnReturn2;

    @Bean(name="dataSource")
    public DruidDataSource getDataSource(){
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setDbType(type);
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setTestOnBorrow(testOnBorrow);
        dataSource.setTestOnReturn(testOnReturn);
        dataSource.setTestWhileIdle(testWhileIdle);
        dataSource.setValidationQuery(validationQuery);
        return  dataSource;
    }

    @Bean(name="dataSource2")
    public DruidDataSource getDataSource2(){
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(url2);
        dataSource.setDbType(type2);
        dataSource.setDriverClassName(driverClassName2);
        dataSource.setUsername(username2);
        dataSource.setPassword(password2);
        dataSource.setTestOnBorrow(testOnBorrow2);
        dataSource.setTestOnReturn(testOnReturn2);
        dataSource.setTestWhileIdle(testWhileIdle2);
        dataSource.setValidationQuery(validationQuery2);
        return  dataSource;
    }

    @Bean(name="jdbcTemplate")
    public JdbcTemplate getJdbcTemplate(@Qualifier("dataSource")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }

    @Bean(name="jdbcTemplate2")
    public JdbcTemplate getJdbcTemplate2(@Qualifier("dataSource2")DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }
}
