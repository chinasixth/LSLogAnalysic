import com.sixth.analysic.model.dim.base.PlatformDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:19 2018/8/21
 * @
 */
public class DimensionTest {
    public static void main(String[] args) {
        PlatformDimension pl = new PlatformDimension("all");
        IDimensionConvert convert = new IDimensionConvertImpl();
        System.out.println(convert.getDimensionByValue(pl));
    }
}
