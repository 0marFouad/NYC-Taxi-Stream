package org.myorg.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Trip {

    private String taxiType;
    private String vendorId;
    private String pickupDateTime;
    private String dropOffDatetime;
    private String pickupLocationId;
    private String dropOffLocationId;
    private String type;

    public String pickupDate;

    public void setTaxiType(String taxiType) { this.taxiType = taxiType; }
    public void setVendorId(String vendorId) { this.vendorId = vendorId; }
    public void setPickupDateTime(String pickupDateTime) {
        this.pickupDateTime = pickupDateTime;
        this.pickupDate = pickupDateTime.split(" ")[0].replace("\"","");
    }
    public void setDropOffDatetime(String dropOffDatetime) { this.dropOffDatetime = dropOffDatetime; }
    public void setPickupLocationId(String pickupLocationId) { this.pickupLocationId = pickupLocationId; }
    public void setDropOffLocationId(String dropOffLocationId) { this.dropOffLocationId = dropOffLocationId; }
    public void setType(String type) { this.type = type; }

    public String getTaxiType() { return taxiType; }
    public String getVendorId() { return vendorId; }
    public String getPickupDateTime() { return pickupDateTime; }
    public String getDropOffDatetime() { return dropOffDatetime; }
    public String getPickupLocationId() { return pickupLocationId; }
    public String getDropOffLocationId() { return dropOffLocationId; }
    public String getType() { return type; }

    public Trip() {}


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