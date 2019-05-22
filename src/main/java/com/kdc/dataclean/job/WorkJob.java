package com.kdc.dataclean.job;


import com.kdc.dataclean.job.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@EnableScheduling
public class WorkJob {

    @Autowired
    private CollectionBasePersonWorkService collectionBasePersonWorkService;

    @Autowired
    private CommunityBasePersonWorkService communityBasePersonWorkService;

    @Autowired
    private ExcelBasePersonWorkService excelBasePersonWorkService;

    @Autowired
    private ExcelFloatPerosnWorkService excelFloatPerosnWorkService;

    @Autowired
    private CommunityDisabledPersonWorkService communityDisabledPersonWorkService;

    @Autowired
    private ExcelDisabledPersonWorkService excelDisabledPersonWorkService;

    @Autowired
    private CommunityOvrseaPersonWorkService communityOvrseaPersonWorkService;

    @Autowired
    private CommunityPartyMbeWorkService communityPartyMbeWorkService;

    @Autowired
    private NonPblicEcnmyOrgWorkService nonPblicEcnmyOrgWorkService;

    // 采集模块人口基础表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  collectionBasePersonExecute(){
        collectionBasePersonWorkService.cleanData();
    }

    // 街道旧智慧社区系统人口基础表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  communityBasePersonExecute(){
        communityBasePersonWorkService.cleanData();
    }

    // 街道excel人口基础表映射
    //  @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  excelBasePersonExecute(){
        excelBasePersonWorkService.cleanData();
    }

    // 街道excel流动人口表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  excelFloatPerosnExecute(){
        excelFloatPerosnWorkService.cleanData();
    }

    // 街道旧智慧社区系统残疾人口表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  communityDisabledPersonExecute(){
        communityDisabledPersonWorkService.cleanData();
    }

    // 街道excel残疾人口表映射
   // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  excelDisabledPersonExecute(){
        excelDisabledPersonWorkService.cleanData();
    }

    // 街道旧智慧社区系统海外人口表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  communityOvrseaPersonExecute(){
        communityOvrseaPersonWorkService.cleanData();
    }

    // 街道旧智慧社区系统党员信息表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  communityPartyMbeExecute(){
        communityPartyMbeWorkService.cleanData();
    }


    // 街道旧智慧社区矫正信息表映射
    // @Scheduled(fixedDelay = 24*60*60*1000)
//     private  void  communityPartyMbeExecute(){
//        communityPartyMbeWorkService.cleanData();
//    }


    // 清洗非公有制经济组织信息表
     // @Scheduled(fixedDelay = 24*60*60*1000)
    private  void  nonPblicEcnmyOrgExecute(){
        nonPblicEcnmyOrgWorkService.cleanData();
    }

}
