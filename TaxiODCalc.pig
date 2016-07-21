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


--- Sample Output (Issues: Rest of data not available) ---
/* ((?BZ9X47,2015-03-16T00:16:31.000Z),10)
((?BZ9X47,2015-03-16T02:20:42.000Z),25)
((?BZ9X47,2015-03-16T08:32:02.000Z),19)
((?BZ9X47,2015-03-16T10:44:08.000Z),20)
((?BZ9X47,2015-03-16T11:37:16.000Z),6)
((?BZ9X47,2015-03-16T14:02:36.000Z),26)
((?BZ9X47,2015-03-16T15:35:15.000Z),10)
((?BZ9X47,2015-03-16T20:21:57.000Z),20)
((?BZ9X47,2015-03-16T23:17:39.000Z),20)
((?BZ9Y41,2015-03-16T01:18:03.000Z),27)
((?BZ9Y41,2015-03-16T08:01:40.000Z),3)
((?BZ9Y41,2015-03-16T09:08:28.000Z),28)
((?BZ9Y41,2015-03-16T10:15:22.000Z),10)
((?BZ9Y41,2015-03-16T12:07:21.000Z),21)
((?BZ9Y41,2015-03-16T12:31:58.000Z),23)
((?BZ9Y41,2015-03-16T13:12:45.000Z),8)
((?BZ9Y41,2015-03-16T13:36:29.000Z),10)
((?BZ9Y41,2015-03-16T13:43:42.000Z),27)
((?BZ9Y41,2015-03-16T17:09:34.000Z),9)
((?BZ9Y41,2015-03-16T17:59:53.000Z),30)
((?BZ9Y41,2015-03-16T18:48:00.000Z),8)
((?BZ9Y41,2015-03-16T21:29:26.000Z),21)
((?BZ9Y47,2015-03-16T08:52:28.000Z),4)
((?BZ9Y47,2015-03-16T09:26:49.000Z),22)
((?BZ9Y47,2015-03-16T12:50:39.000Z),22)
((?BZ9Y47,2015-03-16T19:43:17.000Z),16)
((?BZ9Y47,2015-03-16T21:19:32.000Z),7)
((?BZ9Y47,2015-03-16T21:41:07.000Z),22)
((?BZ9Z39,2015-03-16T21:12:05.000Z),15)
((?BZ9Z39,2015-03-16T22:24:22.000Z),24)
((?BZ9Z39,2015-03-16T22:41:42.000Z),8)
((?BZ9Z49,2015-03-16T13:55:47.000Z),10)
((?BZ9Z49,2015-03-16T17:37:04.000Z),3)
((?BZ9Z49,2015-03-16T23:21:00.000Z),25)
*/
