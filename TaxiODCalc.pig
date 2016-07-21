--- Name: TaxiODCalc-v3
--- Author: Julia Xu
--- Created: 2016-07-21
--- Last Modified: 2016-07-21
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

--- Display complete data set ---
ontimedata = FOREACH grouptimediff {
	ordereddiff = ORDER timediff BY ontimediff ASC;
	cleandata = LIMIT ordereddiff 1;
	GENERATE FLATTEN(cleandata.carid) AS carid,
			 FLATTEN(cleandata.timeon) AS timeon,
			 FLATTEN(cleandata.time) AS time,
			 FLATTEN(cleandata.ontimediff) AS ontimediff,
			 FLATTEN(cleandata.lon) AS lon,
			 FLATTEN(cleandata.lat) AS lat,
			 FLATTEN(cleandata.timeoff) AS timeoff;

};

--DUMP ontimedata;

-------------------Calculating Offtime----------------------------------------------------------------------------------------------------------------------

--- Find 30 seconds difference between offtime and gpstime ---
secbetweenoff = FOREACH data GENERATE carid, timeoff, time, (long)ABS(ISOSecondsBetween(timeoff, time)) AS offtimediff:long, lon, lat, timeon;
filtereddataoff = FILTER secbetweenoff BY offtimediff <= 30;

--- Group by carid and timeoff to find min offtimediff for each timeoff ---
timediffoff = FOREACH filtereddataoff GENERATE carid, timeoff, time, offtimediff, lon, lat, timeon;
grouptimediffoff = GROUP timediffoff BY (carid, timeoff);

--- Display complete data set ---
offtimedata = FOREACH grouptimediffoff {
	ordereddiffoff = ORDER timediffoff BY offtimediff ASC;
	offcleandata = LIMIT ordereddiffoff 1;
	GENERATE FLATTEN(offcleandata.carid) AS carid, 
			 FLATTEN(offcleandata.timeoff) AS timeoff,
			 FLATTEN(offcleandata.time) AS time,
			 FLATTEN(offcleandata.offtimediff) AS offtimediff,
			 FLATTEN(offcleandata.lon) AS lon,
			 FLATTEN(offcleandata.lat) AS lat,
			 FLATTEN(offcleandata.timeon) AS timeon;

};

--DUMP offtimedata;

--- Join ontimedata and offtimedata for final output ---
joindataset = JOIN ontimedata BY timeoff, offtimedata BY timeoff;
completedata = FOREACH joindataset GENERATE ontimedata::carid AS carid:chararray, ontimedata::timeon AS timeon:chararray, ontimedata::time AS otime:chararray, ontimedata::ontimediff AS ontimediff:long, ontimedata::lon AS onlon:double, ontimedata::lat AS onlat:double, 
											ontimedata::timeoff AS offtime:chararray, offtimedata::time AS dtime:chararray, offtimedata::offtimediff AS offtimediff:long, offtimedata::lon AS offlon:double, offtimedata::lat AS offlat:double;

DUMP completedata;
