package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
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

        //1.查询总数
        Integer dauTotal = publisherService.getDauTotal(date);  //日活总数
        Double orderAmountTotal = publisherService.getOrderAmountTotal(date); //日交易额总数


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

        //5.创建Map用于存放新增日交易额数据
        Map<String, Object> orderMap = new HashMap<>();
        orderMap.put("id","order_amount");
        orderMap.put("name","新增交易额");
        orderMap.put("value",orderAmountTotal);

        //5.将Map放入集合
        result.add(dauMap);
        result.add(newMidMap);
        result.add(orderMap);

        //6.返回结果

        return JSONObject.toJSONString(result);
    }

    @RequestMapping("realtime-hours")
    public String getDauTotalHourMap(@RequestParam("id")String id,@RequestParam("date")String date){

        //创建Map用于存放结果数据
        Map<String, Map> result = new HashMap<>();

        if ("dau".equals(id)){

            //1.查询当天分时数据
            Map todayMap = publisherService.getDauTotalHourMap(date);

            //2.查询昨天的分时数据
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();
            Map yesterdayMap = publisherService.getDauTotalHourMap(yesterday);

            //3.将两天数据放入result
            result.put("yesterday",yesterdayMap);
            result.put("today",todayMap);

        } else if ("new_mid".equals(id)) {

            HashMap<String, Long> yesterdayMap = new HashMap<>();
            yesterdayMap.put("09", 100L);
            yesterdayMap.put("12", 200L);
            yesterdayMap.put("17", 150L);

            HashMap<String, Long> todayMap = new HashMap<>();
            todayMap.put("10",400L);
            todayMap.put("13",450L);
            todayMap.put("15",500L);
            todayMap.put("20",600L);

            result.put("yesterday", yesterdayMap);
            result.put("today", todayMap);


        }else if("order_amount".equals(id)){
            //1.查询当天的分时数据
            Map todayMap = publisherService.getOrderAmountHourMap(date);
            //2.查询昨天的分时数据
            String yesterday = LocalDate.parse(date).plusDays(-1).toString();
            Map yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
            //3.将yesterdayMap和todayMap放入result
            result.put("yesterday",yesterdayMap);
            result.put("today",todayMap);
        }
        return JSONObject.toJSONString(result);

    }

    //对realtime-hours请求重构
    @RequestMapping("realtime-hour2")
    public String getDauTotalHourMap2(@RequestParam("id") String id,
                                      @RequestParam("date") String date){

        //创建Map用于存放结果数据
        Map<String, Map> result = new HashMap<>();
        //获取昨天时间
        String yesterday = LocalDate.parse(date).plusDays(-1).toString();
        //声明今天和卓天的分时数据Map
        Map todayMap = null;
        Map yesterdayMap = null;

        if ("dau".equals(id)) {
            //1.查询当天的分时数据
            todayMap = publisherService.getDauTotalHourMap(date);
            //2.查询昨天的分时数据
            yesterdayMap = publisherService.getDauTotalHourMap(yesterday);
        }else if("new_mid".equals(id)){
            //此数据为自造数据
            yesterdayMap = new HashMap<>();
            yesterdayMap.put("09", 100L);
            yesterdayMap.put("12", 200L);
            yesterdayMap.put("17", 150L);

            todayMap = new HashMap<>();
            todayMap.put("10", 400L);
            todayMap.put("13", 450L);
            todayMap.put("15", 500L);
            todayMap.put("20", 600L);
        }else if("order_amount".equals(id)){
            //1.查询当天的分时交易额数据
            todayMap = publisherService.getOrderAmountHourMap(date);
            //2.查询昨天的分时数据
            yesterdayMap = publisherService.getOrderAmountHourMap(yesterday);
        }

        //3.将yesterdayMap和todayMap放入result
        result.put("yesterday",yesterdayMap);
        result.put("today",todayMap);

        return JSONObject.toJSONString(result);

    }

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date") String date,
                                @RequestParam("startpage") int startpage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword) {

        return JSONObject.toJSONString(publisherService.getSaleDetail(date, startpage, size, keyword));
    }


}
