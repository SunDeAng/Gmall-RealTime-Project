package com.atguigu.gmallpublisher.service.impl;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Sdaer
 * @Date: 2020-08-17
 * @Desc:
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;

    @Autowired
    private OrderMapper orderMapper;

    //获取日活总数
    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //获取日活分时数据
    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoenix
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放调整结构后的数据
        Map<String, Long> result = new HashMap<>();

        //3.调整结构
        for (Map map : list) {
            result.put((String)map.get("LH"),(Long)map.get("CT"));
        }

        //4.返回数据
        return result;

    }

    @Override
    public Double getOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHourMap(String date) {

        //1.查询Phoenix
        List<Map> list = orderMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放结果数据
        Map<String, Double> result = new HashMap<>();

        //3.遍历list，调整结构
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回结果
        return result;
    }
}
