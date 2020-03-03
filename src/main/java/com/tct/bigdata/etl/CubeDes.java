package com.tct.bigdata.etl;

import lombok.Data;

import java.util.List;

/**
 * @author yuanhang.liu@tcl.com
 * @description
 * @date 2020-02-26 16:56
 **/
@Data
public class CubeDes {

    private String uuid;
    private String name;
    private String owner;
    private String display_name;
    private Integer cost;
    private String status;
    private List<Segment> segments;
}
