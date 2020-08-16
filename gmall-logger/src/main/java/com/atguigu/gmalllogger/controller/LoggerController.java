package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.atguigu.constant.GmallConstants;

/**
 * @Author: Sdaer
 * @Date: 2020-08-14
 * @Desc:
 */
//@Controller
@RestController //@RestController = @Controller + @ResponseBody
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;



    //@ResponseBody
    @RequestMapping("r1")
    public String testDemo(){
        return "hello demo";
    }

    @RequestMapping("t2")
    public String test2(@RequestParam("name") String nn,@RequestParam("age") int age){
        return "" + nn + ":" + age;
    }

    @RequestMapping("log")
    public String getLog(@RequestParam("logString") String logString){

        //添加时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());

        //写入日志
        log.info(logString);

        //传入kafka
        if ("startup".equals(jsonObject.getString("type"))){
            //kafkaTemplate.send("TOPIC_START",jsonObject.toString());
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonObject.toString());
        }else {
            //kafkaTemplate.send("TOPIC_EVENT", jsonObject.toString());
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonObject.toString());
        }

        return "success";
    }

}
