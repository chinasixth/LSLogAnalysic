package com.sixth.analysic.model.dim.value.reduce;

import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.common.KpiType;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 21:02 2018/8/25
 * @
 */
public class LocalReduceOutputValue extends BaseOutputValueWritable {
    private KpiType kpi;
    private int aus;
    private int sessions;
    private int bounceSessions;

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput, kpi);
        dataOutput.writeInt(this.aus);
        dataOutput.writeInt(this.sessions);
        dataOutput.writeInt(this.bounceSessions);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.kpi = WritableUtils.readEnum(dataInput, KpiType.class);
        this.aus = dataInput.readInt();
        this.sessions = dataInput.readInt();
        this.bounceSessions = dataInput.readInt();
    }

    public LocalReduceOutputValue() {
    }

    public LocalReduceOutputValue(KpiType kpi, int aus, int sessions, int bounceSessions) {
        this.kpi = kpi;
        this.aus = aus;
        this.sessions = sessions;
        this.bounceSessions = bounceSessions;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getAus() {
        return aus;
    }

    public void setAus(int aus) {
        this.aus = aus;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounceSessions() {
        return bounceSessions;
    }

    public void setBounceSessions(int bounceSessions) {
        this.bounceSessions = bounceSessions;
    }
}
