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

--- Sample Output (Issues: Same timeoff but for different cars; some repeated as many matches on timeoff) ---
/* (?B396ZK,2015-03-16T23:40:28.000Z,2015-03-16T23:40:05.000Z,23,114.111839,22.592777,2015-03-16T23:55:51.000Z,2015-03-16T23:55:43.000Z,8,114.048248,22.529034)
(?B396ZK,2015-03-16T23:40:28.000Z,2015-03-16T23:40:05.000Z,23,114.111839,22.592777,2015-03-16T23:55:51.000Z,2015-03-16T23:55:42.000Z,9,113.91993,22.536633)
(?B396ZK,2015-03-16T23:40:28.000Z,2015-03-16T23:40:05.000Z,23,114.111839,22.592777,2015-03-16T23:55:51.000Z,2015-03-16T23:56:07.000Z,16,114.149498,22.556101)
(?BJ3W64,2015-03-16T23:49:55.000Z,2015-03-16T23:49:27.000Z,28,114.03627,22.55875,2015-03-16T23:55:51.000Z,2015-03-16T23:55:43.000Z,8,114.048248,22.529034)
(?BJ3W64,2015-03-16T23:49:55.000Z,2015-03-16T23:49:27.000Z,28,114.03627,22.55875,2015-03-16T23:55:51.000Z,2015-03-16T23:55:42.000Z,9,113.91993,22.536633)
(?BJ3W64,2015-03-16T23:49:55.000Z,2015-03-16T23:49:27.000Z,28,114.03627,22.55875,2015-03-16T23:55:51.000Z,2015-03-16T23:56:07.000Z,16,114.149498,22.556101)
(?BC3N94,2015-03-16T23:48:04.000Z,2015-03-16T23:48:11.000Z,7,113.910332,22.538601,2015-03-16T23:55:51.000Z,2015-03-16T23:55:43.000Z,8,114.048248,22.529034)
(?BC3N94,2015-03-16T23:48:04.000Z,2015-03-16T23:48:11.000Z,7,113.910332,22.538601,2015-03-16T23:55:51.000Z,2015-03-16T23:55:42.000Z,9,113.91993,22.536633)
(?BC3N94,2015-03-16T23:48:04.000Z,2015-03-16T23:48:11.000Z,7,113.910332,22.538601,2015-03-16T23:55:51.000Z,2015-03-16T23:56:07.000Z,16,114.149498,22.556101)
(?B5360D,2015-03-16T23:24:42.000Z,2015-03-16T23:25:00.000Z,18,114.105301,22.5411,2015-03-16T23:55:52.000Z,2015-03-16T23:56:00.000Z,8,114.034119,22.525433)
(?B2G0V5,2015-03-16T23:41:19.000Z,2015-03-16T23:41:45.000Z,26,114.017197,22.638567,2015-03-16T23:55:57.000Z,2015-03-16T23:55:59.000Z,2,114.051636,22.52585)
(?BR11X3,2015-03-16T23:45:11.000Z,2015-03-16T23:45:22.000Z,11,114.09848,22.551817,2015-03-16T23:55:57.000Z,2015-03-16T23:55:59.000Z,2,114.051636,22.52585)
(?B5WT06,2015-03-16T23:54:22.000Z,2015-03-16T23:54:26.000Z,4,114.003304,22.675266,2015-03-16T23:55:59.000Z,2015-03-16T23:56:02.000Z,3,114.07235,22.538267)
(?B5WT06,2015-03-16T23:54:22.000Z,2015-03-16T23:54:26.000Z,4,114.003304,22.675266,2015-03-16T23:55:59.000Z,2015-03-16T23:56:11.000Z,12,114.130386,22.559633)
(?BW7Q09,2015-03-16T23:48:48.000Z,2015-03-16T23:48:28.000Z,20,114.17025,22.56695,2015-03-16T23:56:02.000Z,2015-03-16T23:56:04.000Z,2,114.227867,22.551634)
(?BW7Q09,2015-03-16T23:48:48.000Z,2015-03-16T23:48:28.000Z,20,114.17025,22.56695,2015-03-16T23:56:02.000Z,2015-03-16T23:55:59.000Z,3,114.099533,22.5502)
(?BF8406,2015-03-16T23:36:29.000Z,2015-03-16T23:35:59.000Z,30,114.140533,22.579233,2015-03-16T23:56:02.000Z,2015-03-16T23:56:04.000Z,2,114.227867,22.551634)
(?BF8406,2015-03-16T23:36:29.000Z,2015-03-16T23:35:59.000Z,30,114.140533,22.579233,2015-03-16T23:56:02.000Z,2015-03-16T23:55:59.000Z,3,114.099533,22.5502)
(?BDY968,2015-03-16T23:49:53.000Z,2015-03-16T23:50:20.000Z,27,113.880402,22.581699,2015-03-16T23:56:04.000Z,2015-03-16T23:56:02.000Z,2,113.858398,22.5721)
(?BDY968,2015-03-16T23:49:53.000Z,2015-03-16T23:50:20.000Z,27,113.880402,22.581699,2015-03-16T23:56:04.000Z,2015-03-16T23:55:41.000Z,23,113.877136,22.567898)
(?B1401A,2015-03-16T23:43:38.000Z,2015-03-16T23:43:48.000Z,10,114.062698,22.63905,2015-03-16T23:56:05.000Z,2015-03-16T23:55:40.000Z,25,114.044731,22.565149)
(?B0B0A6,2015-03-16T23:48:52.000Z,2015-03-16T23:49:00.000Z,8,114.125,22.5606,2015-03-16T23:56:10.000Z,2015-03-16T23:56:38.000Z,28,114.130363,22.554016)
(?B0DS75,2015-03-16T23:21:17.000Z,2015-03-16T23:21:37.000Z,20,114.134384,22.570833,2015-03-16T23:56:13.000Z,2015-03-16T23:55:51.000Z,22,114.256584,22.734234)
(?BH1Q28,2015-03-16T23:52:34.000Z,2015-03-16T23:52:24.000Z,10,114.056198,22.6653,2015-03-16T23:56:15.000Z,2015-03-16T23:55:58.000Z,17,113.925117,22.531616)
(?B4VU70,2015-03-16T23:50:13.000Z,2015-03-16T23:50:36.000Z,23,113.932098,22.520399,2015-03-16T23:56:17.000Z,2015-03-16T23:56:47.000Z,30,113.9189,22.5273)
(?B2BS60,2015-03-16T23:50:45.000Z,2015-03-16T23:50:17.000Z,28,113.925034,22.525183,2015-03-16T23:56:20.000Z,2015-03-16T23:56:12.000Z,8,114.024818,22.671667)
(?B2WK57,2015-03-16T23:49:59.000Z,2015-03-16T23:49:48.000Z,11,113.927353,22.685017,2015-03-16T23:56:21.000Z,2015-03-16T23:56:17.000Z,4,113.80088,22.710247)
(?B1ND69,2015-03-16T23:46:01.000Z,2015-03-16T23:45:33.000Z,28,114.123932,22.579184,2015-03-16T23:56:22.000Z,2015-03-16T23:56:51.000Z,29,114.039497,22.525417)
(?B2VU91,2015-03-16T23:39:28.000Z,2015-03-16T23:39:51.000Z,23,114.106331,22.543818,2015-03-16T23:56:22.000Z,2015-03-16T23:56:51.000Z,29,114.039497,22.525417)
(?B3Q4B1,2015-03-16T23:47:31.000Z,2015-03-16T23:47:23.000Z,8,114.080551,22.546749,2015-03-16T23:56:23.000Z,2015-03-16T23:56:49.000Z,26,113.916679,22.520718)
(?BH4Q47,2015-03-16T23:41:56.000Z,2015-03-16T23:41:38.000Z,18,113.914803,22.531,2015-03-16T23:56:23.000Z,2015-03-16T23:56:49.000Z,26,113.916679,22.520718)
(?B2UG85,2015-03-16T23:53:33.000Z,2015-03-16T23:53:37.000Z,4,114.101128,22.546003,2015-03-16T23:56:25.000Z,2015-03-16T23:56:13.000Z,12,114.112213,22.576548)
(?B2UG85,2015-03-16T23:53:33.000Z,2015-03-16T23:53:37.000Z,4,114.101128,22.546003,2015-03-16T23:56:25.000Z,2015-03-16T23:56:01.000Z,24,114.130203,22.640699)
(?B7892D,2015-03-16T23:55:06.000Z,2015-03-16T23:55:04.000Z,2,114.0327,22.615,2015-03-16T23:56:26.000Z,2015-03-16T23:55:58.000Z,28,114.035919,22.522516)
(?BA7P64,2015-03-16T23:41:01.000Z,2015-03-16T23:40:51.000Z,10,114.051132,22.520567,2015-03-16T23:56:27.000Z,2015-03-16T23:56:57.000Z,30,114.1241,22.5462)
(?B6SU77,2015-03-16T23:46:01.000Z,2015-03-16T23:46:26.000Z,25,113.927597,22.5457,2015-03-16T23:56:31.000Z,2015-03-16T23:56:59.000Z,28,114.107964,22.545233)
(?B4825B,2015-03-16T23:52:29.000Z,2015-03-16T23:52:29.000Z,0,114.030319,22.664932,2015-03-16T23:56:35.000Z,2015-03-16T23:56:38.000Z,3,114.166435,22.605612)
(?B045U0,2015-03-16T23:53:55.000Z,2015-03-16T23:54:02.000Z,7,114.158989,22.610027,2015-03-16T23:56:35.000Z,2015-03-16T23:56:38.000Z,3,114.166435,22.605612)
(?B347ZC,2015-03-16T23:42:03.000Z,2015-03-16T23:42:02.000Z,1,114.111748,22.548033,2015-03-16T23:56:37.000Z,2015-03-16T23:56:11.000Z,26,114.082634,22.575451)
(?BR74U5,2015-03-16T23:43:33.000Z,2015-03-16T23:43:36.000Z,3,114.122299,22.541367,2015-03-16T23:56:37.000Z,2015-03-16T23:56:11.000Z,26,114.082634,22.575451)
(?BW8N25,2015-03-16T23:43:52.000Z,2015-03-16T23:43:57.000Z,5,113.923965,22.5296,2015-03-16T23:56:37.000Z,2015-03-16T23:56:11.000Z,26,114.082634,22.575451)
(?B1UE73,2015-03-16T23:49:51.000Z,2015-03-16T23:50:01.000Z,10,114.123016,22.64555,2015-03-16T23:56:37.000Z,2015-03-16T23:56:11.000Z,26,114.082634,22.575451)
(?B183ZD,2015-03-16T23:45:21.000Z,2015-03-16T23:45:41.000Z,20,114.096001,22.631901,2015-03-16T23:56:44.000Z,2015-03-16T23:56:25.000Z,19,114.039001,22.6145)
(?B183ZD,2015-03-16T23:45:21.000Z,2015-03-16T23:45:41.000Z,20,114.096001,22.631901,2015-03-16T23:56:44.000Z,2015-03-16T23:56:18.000Z,26,114.104652,22.544451)
(?B183ZD,2015-03-16T23:45:21.000Z,2015-03-16T23:45:41.000Z,20,114.096001,22.631901,2015-03-16T23:56:44.000Z,2015-03-16T23:56:28.000Z,16,114.114403,22.5443)
(?B8HY13,2015-03-16T23:47:37.000Z,2015-03-16T23:47:39.000Z,2,114.075897,22.536484,2015-03-16T23:56:45.000Z,2015-03-16T23:56:19.000Z,26,113.92765,22.51255)
(?B036Y7,2015-03-16T23:45:37.000Z,2015-03-16T23:45:29.000Z,8,114.044952,22.526934,2015-03-16T23:56:53.000Z,2015-03-16T23:56:47.000Z,6,114.119385,22.540533)
(?B036Y7,2015-03-16T23:45:37.000Z,2015-03-16T23:45:29.000Z,8,114.044952,22.526934,2015-03-16T23:56:53.000Z,2015-03-16T23:56:45.000Z,8,114.183968,22.640751)
(?BN04B3,2015-03-16T23:54:00.000Z,2015-03-16T23:54:19.000Z,19,114.094765,22.559883,2015-03-16T23:56:54.000Z,2015-03-16T23:57:18.000Z,24,114.110435,22.558649)
(?BN04B3,2015-03-16T23:54:00.000Z,2015-03-16T23:54:19.000Z,19,114.094765,22.559883,2015-03-16T23:56:54.000Z,2015-03-16T23:56:54.000Z,0,114.047997,22.5933)
(?B1BX25,2015-03-16T23:52:20.000Z,2015-03-16T23:52:43.000Z,23,114.107803,22.568001,2015-03-16T23:56:54.000Z,2015-03-16T23:57:18.000Z,24,114.110435,22.558649)
(?B1BX25,2015-03-16T23:52:20.000Z,2015-03-16T23:52:43.000Z,23,114.107803,22.568001,2015-03-16T23:56:54.000Z,2015-03-16T23:56:54.000Z,0,114.047997,22.5933)
(?BL7G22,2015-03-16T23:44:43.000Z,2015-03-16T23:44:18.000Z,25,114.012032,22.542784,2015-03-16T23:57:00.000Z,2015-03-16T23:56:52.000Z,8,114.012077,22.633444)
(?BL7G22,2015-03-16T23:44:43.000Z,2015-03-16T23:44:18.000Z,25,114.012032,22.542784,2015-03-16T23:57:00.000Z,2015-03-16T23:56:59.000Z,1,113.877502,22.587799)
(?B4ND87,2015-03-16T23:49:49.000Z,2015-03-16T23:49:52.000Z,3,114.034401,22.625162,2015-03-16T23:57:00.000Z,2015-03-16T23:56:52.000Z,8,114.012077,22.633444)
(?B4ND87,2015-03-16T23:49:49.000Z,2015-03-16T23:49:52.000Z,3,114.034401,22.625162,2015-03-16T23:57:00.000Z,2015-03-16T23:56:59.000Z,1,113.877502,22.587799)
(?B1ND76,2015-03-16T23:50:03.000Z,2015-03-16T23:50:29.000Z,26,113.907249,22.544317,2015-03-16T23:57:02.000Z,2015-03-16T23:57:09.000Z,7,114.086533,22.540617)
(?BN04B9,2015-03-16T23:41:26.000Z,2015-03-16T23:41:26.000Z,0,114.063286,22.5259,2015-03-16T23:57:02.000Z,2015-03-16T23:57:09.000Z,7,114.086533,22.540617)
(?B487YP,2015-03-16T23:51:12.000Z,2015-03-16T23:51:07.000Z,5,113.923721,22.498093,2015-03-16T23:57:08.000Z,2015-03-16T23:57:37.000Z,29,114.10717,22.573732)
(?B487YP,2015-03-16T23:51:12.000Z,2015-03-16T23:51:07.000Z,5,113.923721,22.498093,2015-03-16T23:57:08.000Z,2015-03-16T23:57:15.000Z,7,114.07338,22.623034)
(?B487YP,2015-03-16T23:51:12.000Z,2015-03-16T23:51:07.000Z,5,113.923721,22.498093,2015-03-16T23:57:08.000Z,2015-03-16T23:56:48.000Z,20,113.91629,22.485138)
(?B4FQ28,2015-03-16T23:51:29.000Z,2015-03-16T23:51:11.000Z,18,113.882385,22.588799,2015-03-16T23:57:08.000Z,2015-03-16T23:57:37.000Z,29,114.10717,22.573732)
(?B4FQ28,2015-03-16T23:51:29.000Z,2015-03-16T23:51:11.000Z,18,113.882385,22.588799,2015-03-16T23:57:08.000Z,2015-03-16T23:57:15.000Z,7,114.07338,22.623034)
(?B4FQ28,2015-03-16T23:51:29.000Z,2015-03-16T23:51:11.000Z,18,113.882385,22.588799,2015-03-16T23:57:08.000Z,2015-03-16T23:56:48.000Z,20,113.91629,22.485138)
(?B219P3,2015-03-16T23:52:09.000Z,2015-03-16T23:51:41.000Z,28,114.117531,22.604918,2015-03-16T23:57:09.000Z,2015-03-16T23:57:33.000Z,24,114.125015,22.569799)
(?B7BW42,2015-03-16T23:26:53.000Z,2015-03-16T23:26:47.000Z,6,114.1464,22.555201,2015-03-16T23:57:09.000Z,2015-03-16T23:57:33.000Z,24,114.125015,22.569799)
(?B5WK81,2015-03-16T23:46:29.000Z,2015-03-16T23:46:02.000Z,27,114.019768,22.6343,2015-03-16T23:57:09.000Z,2015-03-16T23:57:33.000Z,24,114.125015,22.569799)
(?B961U9,2015-03-16T23:41:40.000Z,2015-03-16T23:41:11.000Z,29,114.026901,22.6133,2015-03-16T23:57:11.000Z,2015-03-16T23:57:13.000Z,2,114.247437,22.728783)
(?BL7G27,2015-03-16T23:31:28.000Z,2015-03-16T23:31:24.000Z,4,113.960014,22.543051,2015-03-16T23:57:11.000Z,2015-03-16T23:57:13.000Z,2,114.247437,22.728783)
(?B1HU71,2015-03-16T23:49:26.000Z,2015-03-16T23:49:40.000Z,14,113.940948,22.521933,2015-03-16T23:57:11.000Z,2015-03-16T23:57:13.000Z,2,114.247437,22.728783)
(?B0WK42,2015-03-16T23:53:17.000Z,2015-03-16T23:52:50.000Z,27,113.947502,22.5765,2015-03-16T23:57:11.000Z,2015-03-16T23:57:13.000Z,2,114.247437,22.728783)
(?B2BU50,2015-03-16T23:49:43.000Z,2015-03-16T23:49:32.000Z,11,113.961929,22.546967,2015-03-16T23:57:13.000Z,2015-03-16T23:57:02.000Z,11,113.966148,22.532433)
(?B2BU50,2015-03-16T23:49:43.000Z,2015-03-16T23:49:32.000Z,11,113.961929,22.546967,2015-03-16T23:57:13.000Z,2015-03-16T23:57:13.000Z,0,113.967964,22.552668)
(?B2BU50,2015-03-16T23:49:43.000Z,2015-03-16T23:49:32.000Z,11,113.961929,22.546967,2015-03-16T23:57:13.000Z,2015-03-16T23:57:24.000Z,11,113.796967,22.6756)
(?B6NV57,2015-03-16T23:20:48.000Z,2015-03-16T23:20:29.000Z,19,113.880363,22.55265,2015-03-16T23:57:13.000Z,2015-03-16T23:57:02.000Z,11,113.966148,22.532433)
(?B6NV57,2015-03-16T23:20:48.000Z,2015-03-16T23:20:29.000Z,19,113.880363,22.55265,2015-03-16T23:57:13.000Z,2015-03-16T23:57:13.000Z,0,113.967964,22.552668)
(?B6NV57,2015-03-16T23:20:48.000Z,2015-03-16T23:20:29.000Z,19,113.880363,22.55265,2015-03-16T23:57:13.000Z,2015-03-16T23:57:24.000Z,11,113.796967,22.6756)
(?BE5634,2015-03-16T23:46:29.000Z,2015-03-16T23:46:49.000Z,20,114.068115,22.523666,2015-03-16T23:57:19.000Z,2015-03-16T23:57:30.000Z,11,114.056435,22.565817)
(?BZ5Q41,2015-03-16T23:49:22.000Z,2015-03-16T23:48:58.000Z,24,114.087433,22.558434,2015-03-16T23:57:21.000Z,2015-03-16T23:57:01.000Z,20,114.004997,22.686001)
(?BZ5Q41,2015-03-16T23:49:22.000Z,2015-03-16T23:48:58.000Z,24,114.087433,22.558434,2015-03-16T23:57:21.000Z,2015-03-16T23:57:38.000Z,17,113.945335,22.507532)
(?BW5D62,2015-03-16T23:41:45.000Z,2015-03-16T23:41:16.000Z,29,114.068069,22.5284,2015-03-16T23:57:21.000Z,2015-03-16T23:57:01.000Z,20,114.004997,22.686001)
(?BW5D62,2015-03-16T23:41:45.000Z,2015-03-16T23:41:16.000Z,29,114.068069,22.5284,2015-03-16T23:57:21.000Z,2015-03-16T23:57:38.000Z,17,113.945335,22.507532)
(?B142YQ,2015-03-16T23:51:31.000Z,2015-03-16T23:51:13.000Z,18,114.118179,22.539749,2015-03-16T23:57:23.000Z,2015-03-16T23:57:12.000Z,11,114.067497,22.542982)
(?B142YQ,2015-03-16T23:51:31.000Z,2015-03-16T23:51:13.000Z,18,114.118179,22.539749,2015-03-16T23:57:23.000Z,2015-03-16T23:57:51.000Z,28,114.262337,22.711233)
(?B142YQ,2015-03-16T23:51:31.000Z,2015-03-16T23:51:13.000Z,18,114.118179,22.539749,2015-03-16T23:57:23.000Z,2015-03-16T23:57:28.000Z,5,113.961884,22.552532)
(?B3B4C0,2015-03-16T23:46:54.000Z,2015-03-16T23:46:28.000Z,26,114.221436,22.723118,2015-03-16T23:57:23.000Z,2015-03-16T23:57:12.000Z,11,114.067497,22.542982)
(?B3B4C0,2015-03-16T23:46:54.000Z,2015-03-16T23:46:28.000Z,26,114.221436,22.723118,2015-03-16T23:57:23.000Z,2015-03-16T23:57:51.000Z,28,114.262337,22.711233)
(?B3B4C0,2015-03-16T23:46:54.000Z,2015-03-16T23:46:28.000Z,26,114.221436,22.723118,2015-03-16T23:57:23.000Z,2015-03-16T23:57:28.000Z,5,113.961884,22.552532)
(?B703U5,2015-03-16T23:37:49.000Z,2015-03-16T23:38:15.000Z,26,114.139786,22.613882,2015-03-16T23:57:29.000Z,2015-03-16T23:57:06.000Z,23,114.135818,22.551332)
(?B4AE51,2015-03-16T23:51:28.000Z,2015-03-16T23:51:03.000Z,25,113.923103,22.551399,2015-03-16T23:57:33.000Z,2015-03-16T23:57:26.000Z,7,114.129486,22.546766)
(?BJ75B2,2015-03-16T23:42:53.000Z,2015-03-16T23:43:23.000Z,30,114.14122,22.6084,2015-03-16T23:57:33.000Z,2015-03-16T23:57:26.000Z,7,114.129486,22.546766)
(?B0143D,2015-03-16T23:50:42.000Z,2015-03-16T23:50:39.000Z,3,114.048485,22.525734,2015-03-16T23:57:40.000Z,2015-03-16T23:57:16.000Z,24,113.842529,22.603346)
(?B0143D,2015-03-16T23:50:42.000Z,2015-03-16T23:50:39.000Z,3,114.048485,22.525734,2015-03-16T23:57:40.000Z,2015-03-16T23:58:00.000Z,20,114.193199,22.6488)
(?B0143D,2015-03-16T23:50:42.000Z,2015-03-16T23:50:39.000Z,3,114.048485,22.525734,2015-03-16T23:57:40.000Z,2015-03-16T23:57:31.000Z,9,113.881302,22.6028)
(?BV4S27,2015-03-16T23:53:23.000Z,2015-03-16T23:53:44.000Z,21,114.109329,22.591917,2015-03-16T23:57:41.000Z,2015-03-16T23:57:35.000Z,6,113.952118,22.551083)
(?BV4S27,2015-03-16T23:53:23.000Z,2015-03-16T23:53:44.000Z,21,114.109329,22.591917,2015-03-16T23:57:41.000Z,2015-03-16T23:57:43.000Z,2,114.107765,22.547701)
(?BJ74P3,2015-03-16T23:46:37.000Z,2015-03-16T23:46:30.000Z,7,114.132782,22.58095,2015-03-16T23:57:41.000Z,2015-03-16T23:57:35.000Z,6,113.952118,22.551083)
(?BJ74P3,2015-03-16T23:46:37.000Z,2015-03-16T23:46:30.000Z,7,114.132782,22.58095,2015-03-16T23:57:41.000Z,2015-03-16T23:57:43.000Z,2,114.107765,22.547701)
(?B993W7,2015-03-16T23:50:34.000Z,2015-03-16T23:50:16.000Z,18,113.9188,22.5354,2015-03-16T23:57:42.000Z,2015-03-16T23:57:43.000Z,1,114.063133,22.527332)
(?B4AK03,2015-03-16T23:49:12.000Z,2015-03-16T23:49:29.000Z,17,114.045403,22.537399,2015-03-16T23:57:42.000Z,2015-03-16T23:57:43.000Z,1,114.063133,22.527332)
(?B0403A,2015-03-16T23:55:15.000Z,2015-03-16T23:55:00.000Z,15,114.037331,22.639267,2015-03-16T23:57:43.000Z,2015-03-16T23:57:56.000Z,13,114.02533,22.674583)
(?BN1A22,2015-03-16T23:45:41.000Z,2015-03-16T23:45:21.000Z,20,114.084129,22.571117,2015-03-16T23:57:47.000Z,2015-03-16T23:57:28.000Z,19,114.154221,22.558649)
(?B513U7,2015-03-16T23:45:04.000Z,2015-03-16T23:44:55.000Z,9,114.055901,22.618799,2015-03-16T23:57:53.000Z,2015-03-16T23:57:46.000Z,7,114.07222,22.535816)
(?B513U7,2015-03-16T23:45:04.000Z,2015-03-16T23:44:55.000Z,9,114.055901,22.618799,2015-03-16T23:57:53.000Z,2015-03-16T23:57:34.000Z,19,113.9189,22.5431)
(?BV4W17,2015-03-16T23:50:46.000Z,2015-03-16T23:50:54.000Z,8,113.949051,22.542633,2015-03-16T23:57:54.000Z,2015-03-16T23:57:47.000Z,7,113.910347,22.536484)
(?B1HQ81,2015-03-16T23:51:26.000Z,2015-03-16T23:51:17.000Z,9,113.890404,22.5667,2015-03-16T23:57:54.000Z,2015-03-16T23:57:47.000Z,7,113.910347,22.536484)
(?BL2G35,2015-03-16T23:51:54.000Z,2015-03-16T23:51:58.000Z,4,114.086067,22.551666,2015-03-16T23:57:59.000Z,2015-03-16T23:58:19.000Z,20,114.054352,22.56225)
(?B4Q0B3,2015-03-16T23:47:40.000Z,2015-03-16T23:47:31.000Z,9,114.050117,22.542351,2015-03-16T23:57:59.000Z,2015-03-16T23:58:19.000Z,20,114.054352,22.56225)
(?B1G5V6,2015-03-16T23:44:53.000Z,2015-03-16T23:44:49.000Z,4,114.049736,22.567249,2015-03-16T23:58:07.000Z,2015-03-16T23:58:31.000Z,24,114.034134,22.625116)
(?B433YP,2015-03-16T23:31:59.000Z,2015-03-16T23:31:40.000Z,19,114.057983,22.537268,2015-03-16T23:58:07.000Z,2015-03-16T23:58:31.000Z,24,114.034134,22.625116)
(?B914T3,2015-03-16T23:46:58.000Z,2015-03-16T23:47:07.000Z,9,114.045258,22.519548,2015-03-16T23:58:08.000Z,2015-03-16T23:57:52.000Z,16,113.817947,22.673468)
(?B28442,2015-03-16T23:48:33.000Z,2015-03-16T23:48:59.000Z,26,114.113815,22.54245,2015-03-16T23:58:09.000Z,2015-03-16T23:57:50.000Z,19,114.118446,22.540283)
(?B28442,2015-03-16T23:48:33.000Z,2015-03-16T23:48:59.000Z,26,114.113815,22.54245,2015-03-16T23:58:09.000Z,2015-03-16T23:58:32.000Z,23,114.076782,22.535517)
(?B28442,2015-03-16T23:48:33.000Z,2015-03-16T23:48:59.000Z,26,114.113815,22.54245,2015-03-16T23:58:09.000Z,2015-03-16T23:58:08.000Z,1,113.931198,22.549101)
(?B3WK72,2015-03-16T23:53:22.000Z,2015-03-16T23:52:55.000Z,27,113.887199,22.560699,2015-03-16T23:58:09.000Z,2015-03-16T23:57:50.000Z,19,114.118446,22.540283)
(?B3WK72,2015-03-16T23:53:22.000Z,2015-03-16T23:52:55.000Z,27,113.887199,22.560699,2015-03-16T23:58:09.000Z,2015-03-16T23:58:32.000Z,23,114.076782,22.535517)
(?B3WK72,2015-03-16T23:53:22.000Z,2015-03-16T23:52:55.000Z,27,113.887199,22.560699,2015-03-16T23:58:09.000Z,2015-03-16T23:58:08.000Z,1,113.931198,22.549101)
(?B4Q4B0,2015-03-16T23:44:32.000Z,2015-03-16T23:44:23.000Z,9,114.125397,22.542,2015-03-16T23:58:10.000Z,2015-03-16T23:58:26.000Z,16,114.00943,22.63715)
(?BJ95P8,2015-03-16T23:53:08.000Z,2015-03-16T23:53:24.000Z,16,114.083786,22.551701,2015-03-16T23:58:17.000Z,2015-03-16T23:58:31.000Z,14,113.959251,22.592934)
(?BN49Z7,2015-03-16T23:36:39.000Z,2015-03-16T23:36:45.000Z,6,114.036133,22.5245,2015-03-16T23:58:22.000Z,2015-03-16T23:57:56.000Z,26,114.118454,22.579615)
(?BW7Q19,2015-03-16T23:52:33.000Z,2015-03-16T23:52:30.000Z,3,114.116615,22.535967,2015-03-16T23:58:22.000Z,2015-03-16T23:57:56.000Z,26,114.118454,22.579615)
(?B196Y3,2015-03-16T23:47:25.000Z,2015-03-16T23:47:19.000Z,6,114.140671,22.561983,2015-03-16T23:58:23.000Z,2015-03-16T23:58:22.000Z,1,114.092751,22.541597)
(?BD7J04,2015-03-16T23:51:16.000Z,2015-03-16T23:51:23.000Z,7,114.107903,22.571966,2015-03-16T23:58:25.000Z,2015-03-16T23:58:44.000Z,19,114.035919,22.524599)
(?B512V2,2015-03-16T23:56:47.000Z,2015-03-16T23:56:45.000Z,2,113.986572,22.691685,2015-03-16T23:58:25.000Z,2015-03-16T23:58:44.000Z,19,114.035919,22.524599)
(?B044U1,2015-03-16T23:47:15.000Z,2015-03-16T23:46:50.000Z,25,113.927498,22.528189,2015-03-16T23:58:28.000Z,2015-03-16T23:58:51.000Z,23,113.876396,22.563452)
(?B161U1,2015-03-16T23:43:38.000Z,2015-03-16T23:43:52.000Z,14,114.119331,22.539499,2015-03-16T23:58:34.000Z,2015-03-16T23:58:30.000Z,4,114.151421,22.611717)
(?BU8L91,2015-03-16T23:56:16.000Z,2015-03-16T23:55:48.000Z,28,114.12825,22.625067,2015-03-16T23:58:34.000Z,2015-03-16T23:58:30.000Z,4,114.151421,22.611717)
(?BF3020,2015-03-16T23:50:32.000Z,2015-03-16T23:50:02.000Z,30,114.058136,22.535933,2015-03-16T23:58:34.000Z,2015-03-16T23:58:30.000Z,4,114.151421,22.611717)
(?BP44E3,2015-03-16T23:40:32.000Z,2015-03-16T23:40:08.000Z,24,114.127266,22.694151,2015-03-16T23:58:34.000Z,2015-03-16T23:58:30.000Z,4,114.151421,22.611717)
(?B860YU,2015-03-16T23:53:38.000Z,2015-03-16T23:53:48.000Z,10,113.891701,22.5665,2015-03-16T23:58:35.000Z,2015-03-16T23:58:28.000Z,7,114.039734,22.544233)
(?B860YU,2015-03-16T23:53:38.000Z,2015-03-16T23:53:48.000Z,10,113.891701,22.5665,2015-03-16T23:58:35.000Z,2015-03-16T23:58:42.000Z,7,113.893303,22.560101)
(?B97U01,2015-03-16T23:50:59.000Z,2015-03-16T23:50:59.000Z,0,113.900703,22.5641,2015-03-16T23:58:35.000Z,2015-03-16T23:58:28.000Z,7,114.039734,22.544233)
(?B97U01,2015-03-16T23:50:59.000Z,2015-03-16T23:50:59.000Z,0,113.900703,22.5641,2015-03-16T23:58:35.000Z,2015-03-16T23:58:42.000Z,7,113.893303,22.560101)
(?B4912D,2015-03-16T23:53:08.000Z,2015-03-16T23:53:00.000Z,8,114.068298,22.532301,2015-03-16T23:58:35.000Z,2015-03-16T23:58:28.000Z,7,114.039734,22.544233)
(?B4912D,2015-03-16T23:53:08.000Z,2015-03-16T23:53:00.000Z,8,114.068298,22.532301,2015-03-16T23:58:35.000Z,2015-03-16T23:58:42.000Z,7,113.893303,22.560101)
(?B4603B,2015-03-16T23:52:56.000Z,2015-03-16T23:52:35.000Z,21,114.007698,22.658783,2015-03-16T23:58:35.000Z,2015-03-16T23:58:28.000Z,7,114.039734,22.544233)
(?B4603B,2015-03-16T23:52:56.000Z,2015-03-16T23:52:35.000Z,21,114.007698,22.658783,2015-03-16T23:58:35.000Z,2015-03-16T23:58:42.000Z,7,113.893303,22.560101)
(?BL9Z31,2015-03-16T23:50:45.000Z,2015-03-16T23:50:44.000Z,1,114.133034,22.546083,2015-03-16T23:58:36.000Z,2015-03-16T23:58:17.000Z,19,114.03727,22.523916)
(?B7BV40,2015-03-16T23:50:54.000Z,2015-03-16T23:51:07.000Z,13,113.92865,22.552299,2015-03-16T23:58:36.000Z,2015-03-16T23:58:17.000Z,19,114.03727,22.523916)
(?B6HT43,2015-03-16T23:55:24.000Z,2015-03-16T23:55:41.000Z,17,114.082169,22.53405,2015-03-16T23:58:38.000Z,2015-03-16T23:58:27.000Z,11,114.043251,22.542549)
(?B6HT43,2015-03-16T23:55:24.000Z,2015-03-16T23:55:41.000Z,17,114.082169,22.53405,2015-03-16T23:58:38.000Z,2015-03-16T23:58:35.000Z,3,113.963631,22.547068)
(?BA2H94,2015-03-16T23:43:20.000Z,2015-03-16T23:43:37.000Z,17,114.223099,22.688999,2015-03-16T23:58:39.000Z,2015-03-16T23:58:51.000Z,12,114.116531,22.535801)
(?BP14A3,2015-03-16T23:47:42.000Z,2015-03-16T23:47:23.000Z,19,114.046135,22.598516,2015-03-16T23:58:46.000Z,2015-03-16T23:58:17.000Z,29,114.032051,22.631433)
(?B417WZ,2015-03-16T23:51:17.000Z,2015-03-16T23:51:21.000Z,4,114.124535,22.547649,2015-03-16T23:58:47.000Z,2015-03-16T23:59:14.000Z,27,114.137283,22.621082)
(?B417WZ,2015-03-16T23:51:17.000Z,2015-03-16T23:51:21.000Z,4,114.124535,22.547649,2015-03-16T23:58:47.000Z,2015-03-16T23:59:03.000Z,16,114.112251,22.601067)
(?BW9L65,2015-03-16T23:54:35.000Z,2015-03-16T23:54:42.000Z,7,114.032097,22.542017,2015-03-16T23:58:56.000Z,2015-03-16T23:59:16.000Z,20,114.008156,22.551849)
(?B089U5,2015-03-16T23:54:14.000Z,2015-03-16T23:53:54.000Z,20,114.124603,22.552547,2015-03-16T23:58:58.000Z,2015-03-16T23:58:56.000Z,2,114.124809,22.561611)
(?B089U5,2015-03-16T23:54:14.000Z,2015-03-16T23:53:54.000Z,20,114.124603,22.552547,2015-03-16T23:58:58.000Z,2015-03-16T23:59:13.000Z,15,114.111237,22.596201)
(?B2ZY19,2015-03-16T23:51:15.000Z,2015-03-16T23:51:34.000Z,19,113.920197,22.511316,2015-03-16T23:58:58.000Z,2015-03-16T23:58:56.000Z,2,114.124809,22.561611)
(?B2ZY19,2015-03-16T23:51:15.000Z,2015-03-16T23:51:34.000Z,19,113.920197,22.511316,2015-03-16T23:58:58.000Z,2015-03-16T23:59:13.000Z,15,114.111237,22.596201)
(?BJ9Y35,2015-03-16T23:56:47.000Z,2015-03-16T23:56:53.000Z,6,114.124763,22.561972,2015-03-16T23:59:02.000Z,2015-03-16T23:58:42.000Z,20,114.013817,22.550133)
(?B4HP56,2015-03-16T23:50:35.000Z,2015-03-16T23:50:06.000Z,29,114.034752,22.521967,2015-03-16T23:59:05.000Z,2015-03-16T23:58:46.000Z,19,114.116783,22.536133)
(?B3HC41,2015-03-16T23:47:19.000Z,2015-03-16T23:47:12.000Z,7,114.021614,22.639668,2015-03-16T23:59:05.000Z,2015-03-16T23:58:46.000Z,19,114.116783,22.536133)
(?B033V9,2015-03-16T23:49:06.000Z,2015-03-16T23:49:19.000Z,13,114.099915,22.628332,2015-03-16T23:59:06.000Z,2015-03-16T23:59:02.000Z,4,113.90403,22.567249)
(?BA1K41,2015-03-16T23:47:38.000Z,2015-03-16T23:47:40.000Z,2,113.92347,22.488501,2015-03-16T23:59:09.000Z,2015-03-16T23:58:41.000Z,28,114.121147,22.576918)
(?B0ND69,2015-03-16T23:54:28.000Z,2015-03-16T23:53:59.000Z,29,114.101547,22.586367,2015-03-16T23:59:09.000Z,2015-03-16T23:58:41.000Z,28,114.121147,22.576918)
(?BR74X2,2015-03-16T23:40:38.000Z,2015-03-16T23:40:38.000Z,0,114.012016,22.542749,2015-03-16T23:59:17.000Z,2015-03-16T23:59:19.000Z,2,114.135536,22.615267)
(?B2G7V2,2015-03-16T23:39:38.000Z,2015-03-16T23:39:41.000Z,3,114.082001,22.543051,2015-03-16T23:59:17.000Z,2015-03-16T23:59:19.000Z,2,114.135536,22.615267)
(?B0BT45,2015-03-16T23:57:20.000Z,2015-03-16T23:57:02.000Z,18,114.221947,22.554167,2015-03-16T23:59:18.000Z,2015-03-16T23:59:22.000Z,4,114.224884,22.550882)
(?BN74Y5,2015-03-16T23:54:49.000Z,2015-03-16T23:55:07.000Z,18,113.945869,22.5779,2015-03-16T23:59:19.000Z,2015-03-16T23:59:11.000Z,8,113.958702,22.556356)
(?B0MS49,2015-03-16T23:39:21.000Z,2015-03-16T23:39:47.000Z,26,114.055435,22.633333,2015-03-16T23:59:19.000Z,2015-03-16T23:59:11.000Z,8,113.958702,22.556356)
(?B2HU02,2015-03-16T23:51:36.000Z,2015-03-16T23:51:45.000Z,9,114.004997,22.657267,2015-03-16T23:59:23.000Z,2015-03-16T23:59:09.000Z,14,114.021202,22.65695)
(?B255G7,2015-03-16T23:46:46.000Z,2015-03-16T23:46:37.000Z,9,114.221397,22.728634,2015-03-16T23:59:25.000Z,2015-03-16T23:59:16.000Z,9,114.246498,22.728468)
(?B255G7,2015-03-16T23:46:46.000Z,2015-03-16T23:46:37.000Z,9,114.221397,22.728634,2015-03-16T23:59:25.000Z,2015-03-16T23:59:50.000Z,25,114.032585,22.62965)
(?B8GG49,2015-03-16T23:45:23.000Z,2015-03-16T23:45:25.000Z,2,114.06459,22.634111,2015-03-16T23:59:27.000Z,2015-03-16T23:59:32.000Z,5,113.874718,22.584833)
(?B8GG49,2015-03-16T23:45:23.000Z,2015-03-16T23:45:25.000Z,2,114.06459,22.634111,2015-03-16T23:59:27.000Z,2015-03-16T23:59:17.000Z,10,114.011192,22.634527)
(?BR74S2,2015-03-16T23:35:39.000Z,2015-03-16T23:35:21.000Z,18,114.099998,22.541201,2015-03-16T23:59:44.000Z,2015-03-16T23:59:35.000Z,9,114.190498,22.556499)
(?BR74S2,2015-03-16T23:35:39.000Z,2015-03-16T23:35:21.000Z,18,114.099998,22.541201,2015-03-16T23:59:44.000Z,2015-03-16T23:59:47.000Z,3,114.050018,22.549316)
(?BU4K30,2015-03-16T23:58:05.000Z,2015-03-16T23:58:25.000Z,20,114.10688,22.610268,2015-03-16T23:59:53.000Z,2015-03-16T23:59:32.000Z,21,113.919373,22.530991)
(?BV8W37,2015-03-16T23:43:56.000Z,2015-03-16T23:43:30.000Z,26,114.118317,22.558884,2015-03-16T23:59:57.000Z,2015-03-16T23:59:33.000Z,24,114.163383,22.566833)
(?B5FU36,2015-03-16T23:48:10.000Z,2015-03-16T23:47:56.000Z,14,114.119904,22.611292,2015-03-16T23:59:57.000Z,2015-03-16T23:59:33.000Z,24,114.163383,22.566833)
(?BA66F9,2015-03-16T23:46:17.000Z,2015-03-16T23:46:11.000Z,6,114.130051,22.555134,2015-03-16T23:59:57.000Z,2015-03-16T23:59:33.000Z,24,114.163383,22.566833)
(?B007Y5,2015-03-16T23:28:07.000Z,2015-03-16T23:27:49.000Z,18,113.81002,22.626516,2015-03-16T23:59:57.000Z,2015-03-16T23:59:33.000Z,24,114.163383,22.566833)
(?B3WT28,2015-03-16T23:45:19.000Z,2015-03-16T23:45:41.000Z,22,114.03405,22.651318,2015-03-17T00:00:10.000Z,2015-03-16T23:59:57.000Z,13,114.040749,22.608566)
*/
