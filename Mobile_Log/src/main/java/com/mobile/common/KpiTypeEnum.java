package com.mobile.common;

/**
 * kpi的指标类型的枚举
 */
public enum KpiTypeEnum {
    NEW_USER("new_user"),   //用户模块下的新增用户
    BROWSER_NEW_USER("browser_new_user"),   //浏览器模块下的新增用户
    ACTIVE_USER("active_user"),     //活跃用户
    ACTIVE_USER_BROWSER("active_user_browser"),     //浏览器模块下的活跃用户
    TOTAL_NEW_USER("total_new_user"),
    ACTIVE_NUMBER("active_number"),
    ACTIVE_NUMBER_BROWSER("active_number_browser"),
    NEW_MEMBER_BROWSER("new_number_browser"),
    NEW_MEMBER("new_number"),
    SESSION("session"),
    SESSION_BROWSER("session_browser"),
    PAGEVIEW("page_view"),
    LOCATION("location")
    ;

    public String kipName;

    KpiTypeEnum(String kipName) {
        this.kipName = kipName;
    }

    /**
     * 根据传入的kpi名称获取对应的枚举
     * @param kip
     * @return
     */
    public static KpiTypeEnum valueOfKpiName(String kip) {
        for (KpiTypeEnum k : values()) {
            if (k.kipName.equals(kip)) {
                return k;
            }
        }
        return null;
    }
}
