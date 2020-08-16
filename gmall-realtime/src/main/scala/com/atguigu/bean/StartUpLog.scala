package com.atguigu.bean

//在gmall-mock模块中com.atguigu.app.JsonMocker.initStartsupLog
// 可以看到返回日志字段的顺序及详细信息
//此处需与返回的日志字段对应，最后三个需要自己添加
case class StartUpLog(mid: String,    //用户id
                      uid: String,    //设备id
                      appid: String,  //应用id
                      area: String,   //地区，城市
                      os: String,     //系统
                      ch: String,     //todo ch????
                      `type`: String, //日志类型
                      vs: String,     //版本号
                      var logDate: String,
                      var logHour: String,
                      var ts: Long)   //时间戳
