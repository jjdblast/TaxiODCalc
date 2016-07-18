--- Created: 2016-07-15
--- Last Modified: 2016-07-18
--- Summary: Get taxi OD data from GPS and meter info

REGISTER /home/zhangjun/jars/piggybank.jar;
REGISTER /home/zhangjun/joda-time-1.6.jar;
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();

--- Loading data from sources ---
gpsdata = LOAD '/home/xuwenqian/2015-03-16.gz/*' USING PigStorage(',') AS (carid:chararray, type:chararray, company:chararray, lon:double, lat:double, time:chararray);
gpsdeal = LOAD '/home/xuwenqian/2015-03-16/main/part-r-00000' USING PigStorage(',') AS (carid:chararray, date:chararray, timeon:chararray, timeoff:chararray);

--- Join two data groups ---
gpsjoined = JOIN gpsdata BY carid, gpsdeal BY carid;
data = FOREACH gpsjoined GENERATE gpsdeal::carid AS carid:chararray, gpsdeal::timeon AS timeon:chararray, gpsdeal::timeoff AS timeoff:chararray, gpsdata::time AS time:chararray, gpsdata::lon AS lon:double, gpsdata::lat AS lat:double;

--- Find seconds between ---
(long)ABS(ISOSecondsBetween(timeon, time)) AS ontimediff:long;
(long)ABS(ISOSecondsBetween(timeoff, time)) AS offtimediff:long;




--- End Goal: Get longitude and latitude data of both O and D and the time of occurence; known information: carid, ontime, offtime, onlon, onlat, offlon, offlat ---
/*
	STORE var INTO 'file' USING PigStorage(',');
*/

