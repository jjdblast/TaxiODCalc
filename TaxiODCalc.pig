--- Name: TaxiODCalc
--- Author: Julia Xu
--- Created: 2016-07-15
--- Last Modified: 2016-07-18
--- Summary: Get taxi OD data from GPS and meter info


--- Loading data from sources ---
gpsdata = LOAD '/home/xuwenqian/2015-03-16.gz/*' USING PigStorage(',') AS (carid:chararray, type:chararray, company:chararray, lon:double, lat:double, time:chararray);
gpsdeal = LOAD '/home/xuwenqian/2015-03-16/main/part-r-00000' USING PigStorage(',') AS (carid:chararray, date:chararray, timeon:chararray, timeoff:chararray);

--- Join two data groups ---
gpsjoined = JOIN gpsdata BY carid, gpsdeal BY carid;
data = FOREACH gpsjoined GENERATE gpsdeal::carid AS carid:chararray, gpsdeal::timeon AS timeon:chararray, gpsdeal::timeoff AS timeoff:chararray, gpsdata::time AS time:chararray, gpsdata::lon AS lon:double, gpsdata::lat AS lat:double;

--- Converting from string to datetime ---
convtime = FOREACH data GENERATE carid, ToDate(time, 'yyyy/MM/ddTHH:mm:ss') AS (gpstime:datetime), ToDate(timeon, 'yyyy/MM/ddTHH:mm:ss') AS (ontime:datetime), ToDate(timeoff, 'yyyy/MM/ddTHH:mm:ss') AS (offtime:datetime), lon, lat;

--- Join data groups ---
-- convertdata = FOREACH convtime GENERATE data::carid AS carid:chararray, ontime, offtime, gpstime, data::lon AS lon:double, data::lat AS lat:double;

-- convertdata = FOREACH data GENERATE carid AS carid:chararray, convontime::ontime AS ontime:datetime, convofftime::offtime AS offtime:datetime, convtime::gpstime AS gpstime:datetime, lon AS lon:double, lat AS lat:double;

/*
-- Calculate on time difference ---
oncalcdiff = FOREACH convertdata GENERATE ABS(ontime-gpstime) AS ontimediff:long;
onabstimediff = FILTER oncalcdiff BY (ontimediff <= 30);

-- Calculate off time difference ---
offcalcdiff = FOREACH convertdata GENERATE ABS(offtime-gpstime) AS offtimediff:long;
offabstimediff = FILTER offcalcdiff BY (offtimediff <= 30);
*/




--- End Goal: Get longitude and latitude data of both O and D and the time of occurence; known information: carid, ontime, offtime, onlon, onlat, offlon, offlat ---
/*
	STORE var INTO 'file' USING PigStorage(',');
*/

