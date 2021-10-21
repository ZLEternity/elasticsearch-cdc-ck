package com.ctrip.cdc;

/**
 * This is Description
 *
 * @author leon
 * @date 2021/10/12
 */
public class Config {

    private static volatile boolean needInit = true;

    public static boolean isNeedInit() {
        return needInit;
    }

    public static void setNeedInit(boolean needInit) {
        Config.needInit = needInit;
    }
}
