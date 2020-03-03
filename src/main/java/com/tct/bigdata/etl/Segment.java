package com.tct.bigdata.etl;

import lombok.Data;

/**
 * @author yuanhang.liu@tcl.com
 * @description
 * @date 2020-02-26 16:58
 **/
@Data
public class Segment {
    private String uuid;
    private String name;
    private Long date_range_start;
    private Long date_range_end;
    private String status;
}
