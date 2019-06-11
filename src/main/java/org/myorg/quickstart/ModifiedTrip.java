package org.myorg.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ModifiedTrip {

    private String taxiType;
    private String vendorId;
    private int day;
    private long minutes;
    private int dropExist;
    private int isBrooklyn;
    private int BrooklynYellow;
    private int BrooklynGreen;
    private int BrooklynFHV;


    public void setTaxiType(String taxiType) {
        this.taxiType = taxiType;
    }
    public void setVendorId(String vendorId) {
        this.vendorId = vendorId;
    }
    public void setDay(int day) {
        this.day = day;
    }
    public void setMinutes(long minutes) {
        this.minutes = minutes;
    }
    public void setDropExist(int dropExist) {
        this.dropExist = dropExist;
    }
    public void setIsBrooklyn(int isBrooklyn) {
        this.isBrooklyn = isBrooklyn;
    }
    public void setBrooklynYellow(int BrooklynYellow) {
        this.BrooklynYellow = BrooklynYellow;
    }
    public void setBrooklynGreen(int BrooklynGreen) {
        this.BrooklynGreen = BrooklynGreen;
    }
    public void setBrooklynFHV(int BrooklynFHV) {
        this.BrooklynFHV = BrooklynFHV;
    }

    public String getTaxiType() {
        return taxiType;
    }
    public String getVendorId() {
        return vendorId;
    }
    public int getDay() {
        return this.day;
    }
    public float getMinutes() {
        return minutes;
    }
    public int getDropExist() {
        return dropExist;
    }
    public int getIsBrooklyn(){
        return isBrooklyn;
    }
    public int getBrooklynYellow() {
        return BrooklynYellow;
    }
    public int getBrooklynGreen() {
        return BrooklynGreen;
    }
    public int getBrooklynFHV() {
        return BrooklynFHV;
    }



    @Override
    public String toString() {
        try {
            return (new ObjectMapper()).writeValueAsString(this);
//				return  "\n" + taxiType + "\n"
//						+ vendorId + "\n"
//						+ pickupDateTime + "\n"
//						+ dropOffDatetime  + "\n"
//						+ pickupLocationId + "\n"
//						+dropOffLocationId + "\n"
//						+ type
//						+ "\n======================\n";
        } catch (Exception e) {
            throw new RuntimeException("[ERROR] Trip to string: " + e.getMessage());
        }
    }
}