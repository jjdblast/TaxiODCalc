--- Created: 2016-07-18
--- Last Modified: 2016-07-19
--- Summary: Get taxi OD data from GPS and meter info

REGISTER /home/zhangjun/jars/piggybank.jar;
REGISTER /home/xuwenqian/joda-time-1.6.jar;
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();

--- Loading data from sources ---
gpsdata = LOAD '/user/xuwenqian/2015-03-16.gz/*' USING PigStorage(',') AS (carid:chararray, type:chararray, company:chararray, lon:double, lat:double, time:chararray);
gpsdeal = LOAD '/user/xuwenqian/2015-03-16/*' USING PigStorage(',') AS (carid:chararray, date:chararray, timeon:chararray, timeoff:chararray);

--- Join two data groups ---
gpsjoined = JOIN gpsdata BY carid, gpsdeal BY carid;
data = FOREACH gpsjoined GENERATE gpsdeal::carid AS carid:chararray, gpsdeal::timeon AS timeon:chararray, gpsdeal::timeoff AS timeoff:chararray, gpsdata::time AS time:chararray, gpsdata::lon AS lon:double, gpsdata::lat AS lat:double;

--- Find seconds between ontime and gpstime ---
secbetween = FOREACH data GENERATE carid, timeon, time, (long)ABS(ISOSecondsBetween(timeon, time)) AS ontimediff:long, lon, lat, timeoff;

-- Filter converted data set (30 seconds) ---
convdata = FOREACH secbetween GENERATE carid, timeon, time, ontimediff, lon, lat, timeoff;
filtereddata = FILTER convdata BY (ontimediff <= 30);

--- 
timediff = FOREACH filtereddata GENERATE carid, timeon, time, ontimediff, lon, lat, timeoff;

DUMP filtereddata;


--- End Goal: Get longitude and latitude data of both O and D and the time of occurence; known information: carid, ontime, offtime, onlon, onlat, offlon, offlat ---
/*
	STORE var INTO 'file' USING PigStorage(',');
*/
