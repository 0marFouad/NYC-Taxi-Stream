package org.myorg.quickstart;
import org.apache.flink.api.common.functions.*;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.Instant;

public class MapTrip implements MapFunction< Trip,ModifiedTrip > {
    @Override
    public ModifiedTrip map(Trip trip){
        ModifiedTrip newDataTrip = new ModifiedTrip();
        newDataTrip.setMinutes(findTripDuration(trip));
        newDataTrip.setBrooklynYellow(findInLocation(trip,"yellow"));
        newDataTrip.setBrooklynGreen(findInLocation(trip,"green"));
        newDataTrip.setBrooklynFHV(findInLocation(trip,"fhv"));
        newDataTrip.setIsBrooklyn(findInLocation(trip));
        newDataTrip.setTaxiType(trip.getTaxiType());
        newDataTrip.setDay(getDay(trip));
        newDataTrip.setVendorId(trip.getVendorId());
        newDataTrip.setDropExist(isThereDropLoc(trip));
        return newDataTrip;
    }


    public long findTripDuration(Trip trip){
        DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
        long epochDropOffMilli = Instant.from(fmt.parse(trip.getDropOffDatetime().replace("\"", ""))).toEpochMilli();
        long epochPickupMilli = Instant.from(fmt.parse(trip.getPickupDateTime().replace("\"", ""))).toEpochMilli();
        return (epochDropOffMilli - epochPickupMilli) / (60000);
    }

    public int findInLocation(Trip trip){
        if(trip.getPickupLocationId().equals("149") || trip.getPickupLocationId().equals("\"149\"")){
            return 1;
        }
        return 0;
    }

    public int findInLocation(Trip trip,String taxiType){
        if((trip.getPickupLocationId().equals("149") || trip.getPickupLocationId().equals("\"149\"")) && trip.getTaxiType().equals(taxiType)){
            return 1;
        }
        return 0;
    }


    public int getDay(Trip trip){
        System.out.println(trip.pickupDate);
        return Integer.parseInt(trip.pickupDate.split("-")[2]);
    }

    public int isThereDropLoc(Trip trip){
        System.out.println(trip.getDropOffLocationId());
        if(trip.getDropOffLocationId().equals("") || trip.getDropOffLocationId().isEmpty() || trip.getDropOffLocationId().equals("\"\"")){
            return 1;
        }
        return 0;
    }


}
