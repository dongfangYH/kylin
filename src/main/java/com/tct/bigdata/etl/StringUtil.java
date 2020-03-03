package com.tct.bigdata.etl;

import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanhang.liu@tcl.com
 * @description
 * @date 2020-02-17 09:54
 **/
public class StringUtil extends StringUtils {

    public static final String BLANK = "";
    public static final String NULL = "NULL";

    public static String getIfNotEmpty(String value, String defaultValue){
        if (isNoneBlank(value)){
            return value;
        }
        return defaultValue;
    }
}
