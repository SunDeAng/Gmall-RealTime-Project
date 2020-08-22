package com.atguigu.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
/**
 * @Author: Sdaer
 * @Date: 2020-08-22
 * @Desc:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Stat {
    private List<Option> options;
    private String title;
}
