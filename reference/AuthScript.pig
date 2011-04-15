/* this script is designed to create authority metrics on which voters know who will win and how they influence other voters */ 

/*******************************************************************************/

voteTime = LOAD '/data/CMU/hcii/wikipedia/20080312/timeData' USING PigStorage('\t') AS (prospName:chararray, voterName:chararray, vote:int, successNum:int, year:int, month:int, day:int, hour:int, minute:int, utc:long, numSupBef:int, numNeutBef:int, numOppBef:int, numSup:int, numNeut:int, numOpp:int);

voteSlim = FOREACH voteTime GENERATE prospName, voterName, (numSup-numSupBef+0.0)/((numSup-numSupBef) + (numOpp-numOppBef) + 0.0001), (numOpp-numOppBef+0.0)/((numSup-numSupBef) + (numOpp-numOppBef) + 0.0001);

bags = GROUP voteSlim by (voterName);
authorityOverVoters = FOREACH bags GENERATE group, AVG(voteSlim.$2), AVG(voteSlim.$3);

/*******************************************************************************/

votersPros = LOAD '/data/CMU/hcii/wikipedia/20080312/votersPros' USING PigStorage('\t') AS (V_Username:chararray, supportSuccess:int, neutralSuccess:int, opposeSuccess:int, supportFailure:int, neutralFailure:int, opposeFailure:int, total:int);

/* filter voters to those with 5 or more votes */
votersSlimPros = FILTER votersPros BY total >= 5;

authorityOverProspects = FOREACH votersSlimPros GENERATE V_Username, (supportSuccess+0.0)/(supportSuccess+opposeSuccess+0.0001), (opposeFailure+0.0)/(opposeFailure+supportFailure+0.0001);

/*******************************************************************************/

authorityResult = JOIN authorityOverVoters BY $0, authorityOverProspects BY $0;

authorityResultUser = FOREACH authorityResult GENERATE $0, $1, $2, $4, $5;

STORE authorityResultUser INTO '/data/CMU/hcii/wikipedia/20080312/authorityMeasures' USING PigStorage('\t');

/*******************************************************************************/

authorityMeasures = LOAD '/data/CMU/hcii/wikipedia/20080312/authorityMeasures' USING PigStorage('\t') AS (voter:chararray, authVoterSup:double, authVoterOpp:double, authProspSup:double, authProspOpp:double);

copyToLocal /data/CMU/hcii/wikipedia/20080312/authorityMeasures auth;

/*******************************************************************************/

authSup = FOREACH authorityMeasures GENERATE authResSup, authUserSup;
STORE authSup INTO '/data/CMU/hcii/wikipedia/20080312/authSup' USING PigStorage('\t');

authOpp = FOREACH authorityMeasures GENERATE authResOpp, authUserOpp;
STORE authOpp INTO '/data/CMU/hcii/wikipedia/20080312/authOpp' USING PigStorage('\t');

copyToLocal /data/CMU/hcii/wikipedia/20080312/authOpp authOpp;
copyToLocal /data/CMU/hcii/wikipedia/20080312/authSup authSup;

/*******************************************************************************/

rm /data/CMU/hcii/wikipedia/20080312/authorityMeasures
rm /data/CMU/hcii/wikipedia/20080312/authOpp
rm /data/CMU/hcii/wikipedia/20080312/authSup

/*******************************************************************************/

voteTime = LOAD '/data/CMU/hcii/wikipedia/20080312/timeData' USING PigStorage('\t') AS (prospName:chararray, voterNameTime:chararray, vote:int, successNum:int, year:int, month:int, day:int, hour:int, minute:int, utc:long, numSupBef:int, numNeutBef:int, numOppBef:int, numSup:int, numNeut:int, numOpp:int);

voteSlim = FOREACH voteTime GENERATE prospName, voterNameTime, (numSup-numSupBef+0.0)/((numSup-numSupBef) + (numOpp-numOppBef) + 0.0001) AS singleAuthOverVotersSupport, (numOpp-numOppBef+0.0)/((numSup-numSupBef) + (numOpp-numOppBef) + 0.0001) AS singleAuthOverVotersOppose;

bags = GROUP voteSlim by (voterNameTime);

authorityOverVoters = FOREACH bags GENERATE group AS voterNameTime, AVG(voteSlim.singleAuthOverVotersSupport) AS authOverVotersSupport, AVG(voteSlim.singleAuthOverVotersOppose) AS authOverVotersOppose;
DESCRIBE authorityOverVoters;

/*******************************************************************************/

votersPros = LOAD '/data/CMU/hcii/wikipedia/20080312/votersPros' USING PigStorage('\t') AS (voterNameAuth:chararray, supportSuccess:int, neutralSuccess:int, opposeSuccess:int, supportFailure:int, neutralFailure:int, opposeFailure:int, total:int);

/* filter voters to those with 5 or more votes */
votersSlimPros = FILTER votersPros BY total >= 5;

authorityOverResult = FOREACH votersSlimPros GENERATE voterNameAuth, (supportSuccess+0.0)/(supportSuccess+opposeSuccess+0.0001) AS authOverProspectsSuccess, (opposeFailure+0.0)/(opposeFailure+supportFailure+0.0001) AS authOverProspectsFailure;
DESCRIBE authorityOverResult;

/*******************************************************************************/

authority = JOIN authorityOverVoters BY voterNameTime, authorityOverResult BY voterNameAuth;
DESCRIBE authority;

authSupport = FOREACH authority GENERATE authOverProspectsSuccess, authOverVotersSupport;
authOppose = FOREACH authority GENERATE authOverProspectsFailure, authOverVotersOppose;

