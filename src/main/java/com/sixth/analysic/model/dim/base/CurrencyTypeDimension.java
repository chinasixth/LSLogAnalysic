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
 * @ Date   ：Created in 19:20 2018/8/27
 * @
 */
public class CurrencyTypeDimension extends BaseDimension {
    private int id;
    private String currenctName;

    public CurrencyTypeDimension() {
    }

    public CurrencyTypeDimension(String currenctName) {
        this.currenctName = currenctName;
    }

    public CurrencyTypeDimension(int id, String currenctName) {
        this(currenctName);
        this.id = id;
    }

    public static List<CurrencyTypeDimension> buildList(String currencyType) {
        if (StringUtils.isEmpty(currencyType)) {
            currencyType = GlobalConstants.DEFAULT_VALUE;
        }
        List<CurrencyTypeDimension> list = new ArrayList<>();
        list.add(new CurrencyTypeDimension(currencyType));
        list.add(new CurrencyTypeDimension(GlobalConstants.ALL_OF_VALUE));
        return list;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        CurrencyTypeDimension other = (CurrencyTypeDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        return this.currenctName.compareTo(other.currenctName);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.currenctName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.currenctName = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CurrencyTypeDimension that = (CurrencyTypeDimension) o;
        return id == that.id &&
                Objects.equals(currenctName, that.currenctName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, currenctName);
    }

    public int getId() {

        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrenctName() {
        return currenctName;
    }

    public void setCurrenctName(String currenctName) {
        this.currenctName = currenctName;
    }
}
