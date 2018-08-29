package com.sixth.analysic.model.dim.base;

import com.sixth.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 10:20 2018/8/20
 * @
 */
public class KpiDimension extends BaseDimension {
    private int id;
    private String kpiName;

    public KpiDimension() {
    }

    public KpiDimension(String kpiName) {
        this.kpiName = kpiName;
    }

    public KpiDimension(int id, String kpiName) {
        this(kpiName);
        this.id = id;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        KpiDimension other = (KpiDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        return this.kpiName.compareTo(other.kpiName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(kpiName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.kpiName = dataInput.readUTF();
    }

    public List<KpiDimension> buildList(String kpiName) {
        if (StringUtils.isEmpty(kpiName)) {
            kpiName = GlobalConstants.DEFAULT_VALUE;
        }
        List<KpiDimension> list = new ArrayList<>();
        list.add(new KpiDimension(kpiName));
        list.add(new KpiDimension(GlobalConstants.ALL_OF_VALUE));
        return list;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KpiDimension that = (KpiDimension) o;
        return id == that.id &&
                Objects.equals(kpiName, that.kpiName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, kpiName);
    }
}
