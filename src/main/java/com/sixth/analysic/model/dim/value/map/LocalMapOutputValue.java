package com.sixth.analysic.model.dim.value.map;

import com.sixth.analysic.model.dim.value.BaseOutputValueWritable;
import com.sixth.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ Author ：liuhao
 * @ Date   ：Created in 20:57 2018/8/25
 * @
 */
public class LocalMapOutputValue extends BaseOutputValueWritable {
    private String uid; // uuid
    private String sid; // sessionId


    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(uid);
        dataOutput.writeUTF(sid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid = dataInput.readUTF();
        this.sid = dataInput.readUTF();
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }
}
