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
 * @ Date   ：Created in 20:01 2018/8/27
 * @
 */
public class PaymentTypeDimension extends BaseDimension {
    private int id;
    private String paymentType;

    public PaymentTypeDimension() {
    }

    public PaymentTypeDimension(String paymentType) {
        this.paymentType = paymentType;
    }

    public PaymentTypeDimension(int id, String paymentType) {
        this(paymentType);
        this.id = id;
    }

    public static List<PaymentTypeDimension> buildList(String paymentType) {
        if (StringUtils.isEmpty(paymentType)) {
            paymentType = GlobalConstants.DEFAULT_VALUE;
        }
        List<PaymentTypeDimension> list = new ArrayList<>();
        list.add(new PaymentTypeDimension(paymentType));
        list.add(new PaymentTypeDimension(GlobalConstants.ALL_OF_VALUE));
        return list;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        PaymentTypeDimension other = (PaymentTypeDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        return this.paymentType.compareTo(other.paymentType);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.paymentType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.paymentType = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaymentTypeDimension that = (PaymentTypeDimension) o;
        return id == that.id &&
                Objects.equals(paymentType, that.paymentType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, paymentType);
    }

    public int getId() {

        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }
}
