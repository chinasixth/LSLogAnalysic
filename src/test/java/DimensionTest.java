import com.sixth.analysic.model.dim.base.BrowserDimension;
import com.sixth.analysic.model.dim.base.EventDimension;
import com.sixth.analysic.model.dim.base.LocationDimension;
import com.sixth.analysic.mr.service.IDimensionConvert;
import com.sixth.analysic.mr.service.impl.IDimensionConvertImpl;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 11:19 2018/8/21
 * @
 */
public class DimensionTest {
    public static void main(String[] args) {
//        PlatformDimension pl = new PlatformDimension("all");
//        LocationDimension locationDimension = new LocationDimension("山东", "济宁", "曲阜");
        EventDimension eventDimension = new EventDimension("ca", "ac");
        IDimensionConvert convert = new IDimensionConvertImpl();
        System.out.println(convert.getDimensionByValue(eventDimension));
    }
}
