package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import com.google.inject.internal.cglib.core.$LocalVariablesSorter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Author: Sdaer
 * @Date: 2020-08-17
 * @Desc:
 */
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @RequestMapping("realtime-total")
    public String getDauTotal(@RequestParam("date") String date){

        //1.查询日活总数
        Integer dauTotal = publisherService.getDauTotal(date);

        //2.创建List用于存放结果数据
        ArrayList<Map> result = new ArrayList<>();

        //3.创建Map用于存放日活数据
        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","日活数据");
        dauMap.put("value",dauTotal);

        //4.创建Map用于存放新增用户数据
        Map<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增数据");
        newMidMap.put("value",233);

        //5.将Map放入集合
        result.add(dauMap);
        result.add(newMidMap);

        //6.返回结果

        return JSONObject.toJSONString(result);
    }

}