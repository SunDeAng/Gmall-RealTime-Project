package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * @Author: Sdaer
 * @Date: 2020-08-17
 * @Desc:
 */
public interface PublisherService {

    public Integer getDauTotal(String date);

    public Map getDauTotalHourMap(String date);

}
