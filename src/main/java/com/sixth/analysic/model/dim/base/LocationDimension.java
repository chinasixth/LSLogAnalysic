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
 * @ Date   ：Created in 20:05 2018/8/25
 * @
 */
public class LocationDimension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;
    }

    public static List<LocationDimension> buildList(String country, String province, String city) {
        if (StringUtils.isEmpty(country)) {
            country = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(province)) {
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(city)) {
            city = GlobalConstants.DEFAULT_VALUE;
        }
        List<LocationDimension> locationDimensions = new ArrayList<>();
        locationDimensions.add(new LocationDimension(country, province, city));
        locationDimensions.add(new LocationDimension(country, province, GlobalConstants.ALL_OF_VALUE));
        locationDimensions.add(new LocationDimension(country, GlobalConstants.ALL_OF_VALUE, GlobalConstants.ALL_OF_VALUE));

        return locationDimensions;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        LocationDimension other = (LocationDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (tmp != 0) {
            return tmp;
        }
        tmp = this.province.compareTo(other.province);
        if (tmp != 0) {
            return tmp;
        }
        return this.city.compareTo(other.city);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.country);
        dataOutput.writeUTF(this.province);
        dataOutput.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.country = dataInput.readUTF();
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocationDimension that = (LocationDimension) o;
        return id == that.id &&
                Objects.equals(country, that.country) &&
                Objects.equals(province, that.province) &&
                Objects.equals(city, that.city);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, country, province, city);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
