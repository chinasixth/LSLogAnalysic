package com.sixth.analysic.model.dim.base;

import com.sixth.common.GlobalConstants;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 17:10 2018/8/17
 * @ 浏览器维度类
 */
public class BrowserDimension extends BaseDimension {
    private int id;
    private String browserName;
    private String browserVersion;

    // 构建维度集合对象
    public static List<BrowserDimension> buildList(String browserName, String browserVersion) {
        /*
         * 用all去替代一个计算指标时忽略的维度
         *
         * 2018-08-17 website ie 8.0 123
         *
         * 2018-08-17 all ie 8.0 123
         * */

        if (StringUtils.isEmpty(browserName)) {
            browserName = browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(browserVersion)) {
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }
        List<BrowserDimension> list = new ArrayList<>();
        list.add(new BrowserDimension(browserName, browserVersion));
        list.add(new BrowserDimension(browserName, GlobalConstants.ALL_OF_VALUE));

        return list;
    }

    public BrowserDimension() {
    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDimension(int id, String browserName, String browserVersion) {
        this(browserName, browserVersion);
        this.id = id;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        BrowserDimension other = (BrowserDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.browserName.compareTo(other.browserName);
        if (tmp != 0) {
            return tmp;
        }

        return this.browserVersion.compareTo(other.browserVersion);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.browserName);
        dataOutput.writeUTF(this.browserVersion);
    }

    /*
     * 按照write的顺序读取
     * */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.browserName = dataInput.readUTF();
        this.browserVersion = dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowserDimension that = (BrowserDimension) o;
        return id == that.id &&
                Objects.equals(browserName, that.browserName) &&
                Objects.equals(browserVersion, that.browserVersion);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, browserName, browserVersion);
    }
}
