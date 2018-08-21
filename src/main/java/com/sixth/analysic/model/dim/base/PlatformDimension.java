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
 * @ Date   ：Created in 10:12 2018/8/20
 * @
 */
public class PlatformDimension extends BaseDimension {
    private int id;
    private String platformName;

    public PlatformDimension() {
    }

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }

    public PlatformDimension(int id, String platformName) {
        this(platformName);
        this.id = id;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        PlatformDimension other = (PlatformDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        return this.platformName.compareTo(other.platformName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(platformName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.platformName = dataInput.readUTF();
    }

    /*
     * 构建平台维度的集合对象
     * */
    public static List<PlatformDimension> buildList(String platformName) {
        if (StringUtils.isEmpty(platformName)) {
            platformName = GlobalConstants.DEFAULT_VALUE;
        }
        List<PlatformDimension> list = new ArrayList<>();
        list.add(new PlatformDimension(platformName));
        list.add(new PlatformDimension(GlobalConstants.ALL_OF_VALUE));
        return list;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlatformDimension that = (PlatformDimension) o;
        return id == that.id &&
                Objects.equals(platformName, that.platformName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, platformName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }
}
