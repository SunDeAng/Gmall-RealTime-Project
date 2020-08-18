package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * @Author: Sdaer
 * @Date: 2020-08-17
 * @Desc:
 */
public interface PublisherService {

    //对接mapper包中DauMapper的selectDauTotal
    public Integer getDauTotal(String date);

    //对接mapper包中DauMapper的selectDauTotalHourMap
    public Map getDauTotalHourMap(String date);

    //对接mapper包中OrderMapper的selectOrderAmountTotal
    public Double getOrderAmountTotal(String date);

    //对接mapper包中OrderMapper的selectOrderAmountHourMap
    public Map getOrderAmountHourMap(String date);

}
