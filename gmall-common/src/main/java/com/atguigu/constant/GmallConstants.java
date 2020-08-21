package com.atguigu.constant;

/**
 * @Author: Sdaer
 * @Desc:
 */
public class GmallConstants {

    //启动日志主题
    public static final String KAFKA_TOPIC_STARTUP="TOPIC_START";

    //事件日志主题
    public static final String KAFKA_TOPIC_EVENT="TOPIC_EVENT";

    //订单数据主题
    public static final String GMALL_TOPIC_ORDER_INFO="TOPIC_ORDER_INFO";

    //订单详情主题
    public static final String GMALL_TOPIC_ORDER_DETAIL="TOPIC_ORDER_DETAIL";

    //用户信息主题
    public static final String GMALL_TOPIC_USER_INFO = "TOPIC_USER_INFO";

    //ES中预警日志索引前缀
    public static final String GMALL_ES_ALERT_INFO_PRE = "gmall_coupon_alert";


    //ES中销售明细的索引前缀
    public static final String GMALL_ES_SALE_DETAIL_PRE = "gmall200317_sale_detail";

    public static final String ES_INDEX_DAU="gmall2020_dau";
    public static final String ES_INDEX_NEW_MID="gmall2020_new_mid";
    public static final String ES_INDEX_NEW_ORDER="gmall2020_new_order";
    public static final String ES_INDEX_SALE_DETAIL="gmall2020_sale_detail";

}

