package com.mobile.common;

/**
 *
 * 日期类型的枚举
 */
public enum DateType {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public String type ;

    DateType(String type) {
        this.type = type;
    }

    /**
     *
     * 根据传进来的type获取对应的枚举
     * @param type
     * @return
     */
    public static DateType valueOfType(String type) {
        for (DateType date : values()) {
            if (date.type.equals(type)) {
                return date;
            }
        }
        return null;
    }
}
