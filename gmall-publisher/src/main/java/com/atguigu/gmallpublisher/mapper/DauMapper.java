package com.atguigu.gmallpublisher.mapper;

/**
 * @Author: Sdaer
 * @Date: 2020-08-17
 * @Desc:
 */

public interface DauMapper {

    //获取Phoenix表中的日活总数
    public Integer selectDauTotal(String date);

}
