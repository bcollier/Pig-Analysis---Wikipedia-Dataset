
/**************************************************************************************/

pig -Dmapred.job.queue.name=m45
copyFromLocal timeData /data/CMU/hcii/wikipedia/20080312/timeData

cd myudfs
javac -cp myudfs/pig.jar myudfs/SETNUMB.java
javac -cp myudfs/pig.jar myudfs/UPPER.java
jar -cf myudfs.jar myudfs

REGISTER myudfs.jar;

voteTime = LOAD '/data/CMU/hcii/wikipedia/20080312/timeData' USING PigStorage('\t') AS (prospName:chararray, voterName:chararray, vote:int, successNum:int, year:int, month:int, day:int, hour:int, minute:int, utc:long, numSupBef:int, numNeutBef:int, numOppBef:int, numSup:int, numNeut:int, numOpp:int);

res = FOREACH voteTime GENERATE myudfs.UPPER(prospName), voterName;

/**************************************************************************************/

voteTime = LOAD '/data/CMU/hcii/wikipedia/20080312/timeData' USING PigStorage('\t') AS (prospName:chararray, voterName:chararray, vote:int, successNum:int, year:int, month:int, day:int, hour:int, minute:int, utc:long, numSupBef:int, numNeutBef:int, numOppBef:int, numSup:int, numNeut:int, numOpp:int);

voteSlim = FOREACH voteTime GENERATE prospName, voterName, (numSup-numSupBef+0.0)/((numSup-numSupBef) + (numOpp-numOppBef) + 0.0001), (numOpp-numOppBef+0.0)/((numSup-numSupBef) + (numOpp-numOppBef) + 0.0001);

bags = GROUP voteSlim by (voterName);

authorityOverUsers = FOREACH bags GENERATE group, AVG(voteSlim.$2), AVG(voteSlim.$3);

/**************************************************************************************/

votersPros = LOAD '/data/CMU/hcii/wikipedia/20080312/votersPros' USING PigStorage('\t') AS (V_Username:chararray, supportSuccess:int, neutralSuccess:int, opposeSuccess:int, supportFailure:int, neutralFailure:int, opposeFailure:int, total:int);

/* filter voters to those with 5 or more votes */
votersSlimPros = FILTER votersPros BY total >= 5;

authorityOverResult = FOREACH votersSlimPros GENERATE V_Username, (supportSuccess+0.0)/(supportSuccess+supportFailure+0.0001), (opposeFailure+0.0)/(opposeFailure+opposeSuccess+0.0001);

/**************************************************************************************/

authorityResult = JOIN authorityOverUsers BY $0, authorityOverResult BY $0;
authorityResultUser = FOREACH authorityResult GENERATE $0, $1, $2, $4, $5;

STORE authorityResultUser INTO '/data/CMU/hcii/wikipedia/20080312/authorityMeasures' USING PigStorage('\t');

authorityMeasures = LOAD '/data/CMU/hcii/wikipedia/20080312/authorityMeasures' USING PigStorage('\t') AS (voter:chararray, authUserSup:double, authUserOpp:double, authResSup:double, authResOpp:double);

authSup = FOREACH authorityMeasures GENERATE authResSup, authUserSup;
STORE authSup INTO '/data/CMU/hcii/wikipedia/20080312/authSup' USING PigStorage('\t');

authOpp = FOREACH authorityMeasures GENERATE authResOpp, authUserOpp;
STORE authOpp INTO '/data/CMU/hcii/wikipedia/20080312/authOpp' USING PigStorage('\t');

copyToLocal /data/CMU/hcii/wikipedia/20080312/authOpp authOpp;
copyToLocal /data/CMU/hcii/wikipedia/20080312/authSup authSup;

/**************************************************************************************/


