import com.mobile.etl.util.IpUtil;
import com.mobile.etl.util.ip.IPSeeker;

import java.util.List;

public class TestIpUtil {
    public static void main(String[] args) {
////        System.out.println(IPSeeker.getInstance().getArea("183.62.92.3"));
////        System.out.println(IPSeeker.getInstance().getAddress("183.62.92.3"));
////        System.out.println(IpUtil.parserIp("192.168.216.111"));
//        System.out.println(IpUtil.parserIp("183.62.92.3"));
//        System.out.println(IpUtil.parserIp("192.168.216.111"));
//        System.out.println(IpUtil.parserIp("117.15.255.255"));
//        System.out.println(IpUtil.parserIp("59.48.173.255"));
//        System.out.println(IpUtil.parserIp("211.139.77.41"));
//        System.out.println(IpUtil.parserIp("222.217.255.255"));
        List<String> list = IPSeeker.getInstance().getAllIp();
        for (String m : list){
            System.out.println(IpUtil.parserIp(m));
        }


    }
}
