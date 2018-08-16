import com.sixth.etl.util.IPParseUtil;
import com.sixth.etl.util.ip.IPSeeker;

import java.util.List;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 16:41 2018/8/15
 * @
 */
public class IpTest {
    public static void main(String[] args) {
        System.out.println(new IPParseUtil().parserIp("221.11.112.123"));
        System.out.println(new IPParseUtil().parserIp("118.230.11.123"));
        System.out.println(new IPParseUtil().parserIp("221.13.95.123"));
//        IPParseUtil ipParseUtil = new IPParseUtil();
        List<String> li = IPSeeker.getInstance().getAllIp();
        Long count = 0L;
        for (String ip : li) {
            System.out.println(ip+"====" + new IPParseUtil().parserIp(ip)+"       " + count+1);
            count ++;
        }
    }
}
