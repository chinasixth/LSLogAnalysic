import com.sixth.etl.util.UserAgentUtil;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 15:40 2018/8/16
 * @
 */
public class UserAgentUtilTest {
    public static void main(String[] args) {
        System.out.println(
                new UserAgentUtil()
                        .parseUserAgent(
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299"));

        System.out.println(new UserAgentUtil().parseUserAgent("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)"));
        System.out.println(new UserAgentUtil().parseUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/14.0.835.163 Safari/535.1"));
        System.out.println(new UserAgentUtil().parseUserAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:6.0) Gecko/20100101 Firefox/6.0"));
    }
}
