package com.mobile.common;

public enum EnumEvent {
    LANUCH(1, "launch_event", "e_l"),
    PAGEVIEW(2,"page_view_event", "e_pv"),
    CHARGE_REQUEST(3, "charge_request_event", "e_crt"),
    CHARGE_SUCCESS(4, "charge_success_event", "e_cs"),
    CHARGE_REFUND(5, "charge_refund_event", "e_cr"),
    EVENT(6, "event", "e_e");

    public int id;
    public String eventName;
    public String alias;

    EnumEvent(int id, String eventName, String alias) {
        this.id = id;
        this.eventName = eventName;
        this.alias = alias;
    }

    //根据枚举的别名获取枚举
    public static EnumEvent valueOfAlias(String aliasName) {
        //循环所有的枚举
        for (EnumEvent event : values()) {
            if (event.alias.equals(aliasName)) {
                return event;
            }
        }

        throw new RuntimeException("aliasName暂时没有对应的Event事件");
    }

}
