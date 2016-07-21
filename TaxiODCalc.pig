--- Name: TaxiODCalc-v2
--- Author: Julia Xu
--- Created: 2016-07-18
--- Last Modified: 2016-07-20
--- Summary: Get taxi OD data from GPS and meter info

REGISTER /home/zhangjun/jars/piggybank.jar;
REGISTER /home/xuwenqian/joda-time-1.6.jar;
DEFINE ISOSecondsBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween();

--- Loading data from sources ---
gpsdata = LOAD '/user/xuwenqian/2015-03-16.gz/part-m-00000.gz' USING PigStorage(',') AS (carid:chararray, type:chararray, company:chararray, lon:double, lat:double, time:chararray);
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

DUMP ontimedata;

-------------------------------------------------------------------------------------------------------------------------------------------------

--- Find 30 seconds difference between offtime and gpstime ---
-- offdata = FOREACH ontimedata GENERATE carid, timeon, time, ontimediff, lon, lat, timeoff, 


--- Find seconds between offtime and gpstime ---
/*
	Load from gpsdata and gpsdeal. Match with timeoff. 

--- End Goal: Get longitude and latitude data of both O and D and the time of occurence; known information: carid, ontime, offtime, onlon, onlat, offlon, offlat ---
/*
	STORE var INTO 'file' USING PigStorage(',');
*/



--- Sample Output (SUCESS!! (only with timeon - not timeoff) ---
/*
(?BZ9Y41,2015-03-16T01:18:03.000Z,2015-03-16T01:18:30.000Z,27,114.122086,22.60885,2015-03-16T01:29:24.000Z)
(?BZ9Y41,2015-03-16T08:01:40.000Z,2015-03-16T08:01:43.000Z,3,114.034714,22.624832,2015-03-16T08:15:38.000Z)
(?BZ9Y41,2015-03-16T09:08:28.000Z,2015-03-16T09:08:00.000Z,28,114.133232,22.617001,2015-03-16T09:30:31.000Z)
(?BZ9Y41,2015-03-16T10:15:22.000Z,2015-03-16T10:15:32.000Z,10,114.131248,22.54825,2015-03-16T10:18:43.000Z)
(?BZ9Y41,2015-03-16T12:07:21.000Z,2015-03-16T12:07:00.000Z,21,114.106949,22.56275,2015-03-16T12:25:53.000Z)
(?BZ9Y41,2015-03-16T12:31:58.000Z,2015-03-16T12:31:35.000Z,23,114.117668,22.558968,2015-03-16T12:37:58.000Z)
(?BZ9Y41,2015-03-16T13:12:45.000Z,2015-03-16T13:12:53.000Z,8,114.111832,22.585882,2015-03-16T13:21:37.000Z)
(?BZ9Y41,2015-03-16T13:36:29.000Z,2015-03-16T13:36:39.000Z,10,114.124313,22.572983,2015-03-16T13:41:35.000Z)
(?BZ9Y41,2015-03-16T13:43:42.000Z,2015-03-16T13:44:09.000Z,27,114.133835,22.564301,2015-03-16T13:50:29.000Z)
(?BZ9Y41,2015-03-16T17:09:34.000Z,2015-03-16T17:09:25.000Z,9,114.120316,22.5446,2015-03-16T17:14:21.000Z)
(?BZ9Y41,2015-03-16T17:59:53.000Z,2015-03-16T18:00:23.000Z,30,114.097267,22.567083,2015-03-16T18:06:25.000Z)
(?BZ9Y41,2015-03-16T18:48:00.000Z,2015-03-16T18:48:08.000Z,8,114.117966,22.573517,2015-03-16T18:55:39.000Z)
(?BZ9Y41,2015-03-16T21:29:26.000Z,2015-03-16T21:29:47.000Z,21,113.991936,22.539734,2015-03-16T21:51:56.000Z)
(?BZ9Y47,2015-03-16T08:52:28.000Z,2015-03-16T08:52:24.000Z,4,114.117584,22.54435,2015-03-16T09:02:46.000Z)
(?BZ9Y47,2015-03-16T09:26:49.000Z,2015-03-16T09:27:11.000Z,22,114.071884,22.531166,2015-03-16T09:37:54.000Z)
(?BZ9Y47,2015-03-16T12:50:39.000Z,2015-03-16T12:51:01.000Z,22,114.018501,22.539083,2015-03-16T13:00:27.000Z)
(?BZ9Y47,2015-03-16T19:43:17.000Z,2015-03-16T19:43:01.000Z,16,113.951164,22.5604,2015-03-16T19:54:55.000Z)
(?BZ9Y47,2015-03-16T21:19:32.000Z,2015-03-16T21:19:39.000Z,7,114.1241,22.579317,2015-03-16T21:36:46.000Z)
(?BZ9Y47,2015-03-16T21:41:07.000Z,2015-03-16T21:41:29.000Z,22,114.032768,22.541317,2015-03-16T21:44:56.000Z)
(?BZ9Z39,2015-03-16T21:12:05.000Z,2015-03-16T21:11:50.000Z,15,114.0989,22.5714,2015-03-16T21:37:09.000Z)
(?BZ9Z39,2015-03-16T22:24:22.000Z,2015-03-16T22:23:58.000Z,24,114.116898,22.5394,2015-03-16T22:33:44.000Z)
(?BZ9Z39,2015-03-16T22:41:42.000Z,2015-03-16T22:41:34.000Z,8,114.106102,22.5462,2015-03-16T22:47:54.000Z)
(?BZ9Z49,2015-03-16T13:55:47.000Z,2015-03-16T13:55:37.000Z,10,113.856102,22.617001,2015-03-16T14:15:12.000Z)
(?BZ9Z49,2015-03-16T17:37:04.000Z,2015-03-16T17:37:01.000Z,3,114.059196,22.5354,2015-03-16T18:04:10.000Z)
(?BZ9Z49,2015-03-16T23:21:00.000Z,2015-03-16T23:20:35.000Z,25,113.810097,22.627501,2015-03-16T23:48:51.000Z)
*/
