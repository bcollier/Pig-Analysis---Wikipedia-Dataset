/* this is to get data on the effects voters have on prospects */

/* create voter info as to whether support/oppose contributed to success/failure */
/* votersPros */

VAData = LOAD '/data/CMU/hcii/wikipedia/20080312/VAData' USING PigStorage('\t') AS (SupOpN:int, VoterID0:int, rfaid0:int, C_Username, V_Username, AdminAtVote, VoterID1, censor, comment, twiceinonemonth, badcontribdata, datename, username, alias, rfamonth, rfayear, rfamonthnum, rfa_yearmonth, monthssincefirstedit, editspermonth, attemptnum, totaledits, minoredits, articleedits, atalkedits, useredits, utalkedits, wpedits, wptalkedits, pctminoredits, pctarticleedits, pctatalkedits, pctutalkedits, pctwpedits, pctwptalkedits, pctanytalkedits, pctwporwptalk, pctuseredits, tenpctdiversity, twentypctdiversity, twentyfivepctdiversity, fivehundiversity, hundreddiversity, autocomments, totalComments, humanWrittenComments, pctwrittencomments, pctcommented, newLengthInWords, LengthInChars, log2commentlength, UniqueWords, please, thanks, vandal, revert, pov, pagescreated, pctunique, pctplease, pctthanks, pctvandal, pctrevert, pctpov, hasplease, hasthanks, hasvandal, hasrevert, haspov, firstedit, firstedityear, firsteditmonth, xfd, deletionall, rfc, otherrfas, vpump, votes, aiv, rfprotection, noticeboard, wikiquette, welcome, adminattn, arb, mediation, wikiproject, arb_or_mediation, CD, success, successNum, rfaid1);

VADataSmall = FOREACH VAData GENERATE C_Username, V_Username, SupOpN, successNum, rfa_yearmonth;

/* only use 200707 */
VADataDate = FILTER VADataSmall BY (rfa_yearmonth == '200705' OR rfa_yearmonth == '200706' OR rfa_yearmonth == '200707');

voterGroup = GROUP VADataDate BY (SupOpN, successNum, V_Username);
voterTotal = FOREACH voterGroup GENERATE FLATTEN(group), COUNT(VADataDate) as count;

/* seperate each of six features to be included */
supportSuccess = FILTER voterTotal BY (SupOpN == -1) AND (successNum == 0);
neutralSuccess = FILTER voterTotal BY (SupOpN == 0) AND (successNum == 0);
opposeSuccess = FILTER voterTotal BY (SupOpN == 1) AND (successNum == 0);
supportFailure = FILTER voterTotal BY (SupOpN == -1) AND (successNum == 1);
neutralFailure = FILTER voterTotal BY (SupOpN == 0) AND (successNum == 1);
opposeFailure = FILTER voterTotal BY (SupOpN == 1) AND (successNum == 1);

/* we care about the number of prospects (V_Username) in each case of (SupOpN, successNum), i.e. {(-1,1) (0,1) (1,1) (-1,0) (0,0) (1,0)} */
supportSuccess = FOREACH supportSuccess GENERATE V_Username, count;
neutralSuccess = FOREACH neutralSuccess GENERATE V_Username, count;
opposeSuccess = FOREACH opposeSuccess GENERATE V_Username, count;
supportFailure = FOREACH supportFailure GENERATE V_Username, count;
neutralFailure = FOREACH neutralFailure GENERATE V_Username, count;
opposeFailure = FOREACH opposeFailure GENERATE V_Username, count;

/* create each value, NULLs mean that voter had 0 entries (prospects) for that case */
voters = JOIN supportSuccess BY V_Username FULL,neutralSuccess BY V_Username;
SPLIT voters INTO v1Blank IF ($0 is null), v2Blank IF ($2 is null), noBlank IF (($0 is not null) AND ($2 is not null));
v1Blank = FOREACH v1Blank GENERATE $2 as V_Username, 0,  $3;
v2Blank = FOREACH v2Blank GENERATE $0 as V_Username, $1, 0;
noBlank = FOREACH noBlank GENERATE $0 as V_Username, $1, $3;
voters = UNION v1Blank, v2Blank, noBlank;

voters = JOIN voters BY V_Username FULL, opposeSuccess BY V_Username;
SPLIT voters INTO v1Blank IF ($0 is null), v2Blank IF ($3 is null), noBlank IF (($0 is not null) AND ($3 is not null));
v1Blank = FOREACH v1Blank GENERATE $3 as V_Username, 0,0,   $4;
v2Blank = FOREACH v2Blank GENERATE $0 as V_Username, $1,$2, 0;
noBlank = FOREACH noBlank GENERATE $0 as V_Username, $1,$2, $4;
voters = UNION v1Blank, v2Blank, noBlank;

voters = JOIN voters BY V_Username FULL, supportFailure BY V_Username;
SPLIT voters INTO v1Blank IF ($0 is null), v2Blank IF ($4 is null), noBlank IF (($0 is not null) AND ($4 is not null));
v1Blank = FOREACH v1Blank GENERATE $4 as V_Username, 0,0,0,    $5;
v2Blank = FOREACH v2Blank GENERATE $0 as V_Username, $1,$2,$3, 0;
noBlank = FOREACH noBlank GENERATE $0 as V_Username, $1,$2,$3, $5;
voters = UNION v1Blank, v2Blank, noBlank;

voters = JOIN voters BY V_Username FULL, neutralFailure BY V_Username;
SPLIT voters INTO v1Blank IF ($0 is null), v2Blank IF ($5 is null), noBlank IF (($0 is not null) AND ($5 is not null));
v1Blank = FOREACH v1Blank GENERATE $5 as V_Username, 0,0,0,0,     $6;
v2Blank = FOREACH v2Blank GENERATE $0 as V_Username, $1,$2,$3,$4, 0;
noBlank = FOREACH noBlank GENERATE $0 as V_Username, $1,$2,$3,$4, $6;
voters = UNION v1Blank, v2Blank, noBlank;

voters = JOIN voters BY V_Username FULL, opposeFailure BY V_Username;
SPLIT voters INTO v1Blank IF ($0 is null), v2Blank IF ($6 is null), noBlank IF (($0 is not null) AND ($6 is not null));
v1Blank = FOREACH v1Blank GENERATE $6 as V_Username, 0,0,0,0,0,      $7;
v2Blank = FOREACH v2Blank GENERATE $0 as V_Username, $1,$2,$3,$4,$5, 0;
noBlank = FOREACH noBlank GENERATE $0 as V_Username, $1,$2,$3,$4,$5, $7;
voters = UNION v1Blank, v2Blank, noBlank;

/* supportSuccess */
/* neutralSuccess */
/* opposeSuccess */
/* supportFailure */
/* neutralFailure */
/* opposeFailure */

/* get total */
voters = FOREACH voters GENERATE $0,$1,$2,$3,$4,$5,$6,($1+$2+$3+$4+$5+$6);
STORE voters INTO '/data/CMU/hcii/wikipedia/20080312/votersPros07' USING PigStorage('\t');

/************************************************************************************/

voters = LOAD '/data/CMU/hcii/wikipedia/20080312/votersPros07' USING PigStorage('\t') AS (voter:chararray, supportSuccess:float, neutralSuccess:float, opposeSuccess:float, supportFailure:float, neutralFailure:float, opposeFailure:float, totalVotes:float);

votersDes = FOREACH voters GENERATE voter, supportSuccess/(supportSuccess + supportFailure + 0.0001), opposeFailure/(opposeFailure + opposeSuccess + 0.0001),supportSuccess + supportFailure,opposeFailure + opposeSuccess;

STORE votersDes INTO '/data/CMU/hcii/wikipedia/20080312/votersProsAnal07' USING PigStorage('\t');


