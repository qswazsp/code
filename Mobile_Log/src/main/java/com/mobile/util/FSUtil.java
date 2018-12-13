package com.mobile.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;


public class FSUtil {
    private static Logger logger = Logger.getLogger(FSUtil.class);

    /**
     *
     * 获取fs对象
     * @return
     */
    public static FileSystem getFS() {
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            logger.warn("获取fs对象异常", e);
        }
        return fs;
    }

    public static void closeFS(FileSystem fileSystem) {
        if (fileSystem != null) {
            try {
                fileSystem.close();
            } catch (IOException e) {
                //do nothing
            }
        }

    }


}
