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
    public static final String KAFKA_TOPIC_ORDER_DETAIL="GMALL_ORDER_DETAIL";

    //ES中预警日志索引前缀
    public static final String GMALL_ES_ALERT_INFO_PRE = "gmall_coupon_alert";

    public static final String ES_INDEX_DAU="gmall2020_dau";
    public static final String ES_INDEX_NEW_MID="gmall2020_new_mid";
    public static final String ES_INDEX_NEW_ORDER="gmall2020_new_order";
    public static final String ES_INDEX_SALE_DETAIL="gmall2020_sale_detail";

}

