package com.tct.bigdata.etl;

import lombok.Data;

/**
 * @author yuanhang.liu@tcl.com
 * @description
 * @date 2020-02-13 17:16
 **/
@Data
public class MidEvent {
    private String appName;
    private String appVersion;
    private String teyeId;
    private String deviceName;

    public MidEvent(String appName, String appVersion, String teyeId, String deviceName) {
        this.appName = appName;
        this.appVersion = appVersion;
        this.teyeId = teyeId;
        this.deviceName = deviceName;
    }
}
