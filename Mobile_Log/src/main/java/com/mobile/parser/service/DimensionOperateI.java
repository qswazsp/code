package com.mobile.parser.service;

import com.mobile.parser.modle.dim.base.BaseDimension;

/**
 *
 * 操作基础维度类的接口
 */
public interface DimensionOperateI {
    /**
     * 根据传入的维度对象获取其对应的维度id
     * @param dimension     传入一个维度
     * @return  返回各个对象的id值
     */
    int getDimensionIdByDimension(BaseDimension dimension);



}
