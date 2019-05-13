package com.kdc.dataclean.job;


import com.kdc.dataclean.job.service.WorkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

@Component
@EnableScheduling
public class WorkJob {
    @Autowired
    private WorkService workService;

    @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  main() throws Exception {
        workService.cleanData();
    }
}
