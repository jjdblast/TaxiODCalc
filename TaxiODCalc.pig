--- Name: TaxiODCalc-v2
--- Author: Julia Xu
--- Created: 2016-07-18
--- Last Modified: 2016-07-20
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

--- Find 30 seconds difference between ontime and gpstime ---
secbetween = FOREACH data GENERATE carid, timeon, time, (long)ABS(ISOSecondsBetween(timeon, time)) AS ontimediff:long, lon, lat, timeoff;
filtereddata = FILTER secbetween BY ontimediff <= 30;

--- Group by carid and timeon to find min ontimediff for each timeon ---
timediff = FOREACH filtereddata GENERATE carid, timeon, time, ontimediff, lon, lat, timeoff;
grouptimediff  = GROUP timediff BY (carid, timeon);
findmin = FOREACH grouptimediff GENERATE group as grp,MIN(timediff.ontimediff) AS mindiff;


DUMP findmin;

--- Find seconds between offtime and gpstime ---
/*
	Load from gpsdata and gpsdeal. Match with timeoff. 

--- End Goal: Get longitude and latitude data of both O and D and the time of occurence; known information: carid, ontime, offtime, onlon, onlat, offlon, offlat ---
/*
	STORE var INTO 'file' USING PigStorage(',');
*/
