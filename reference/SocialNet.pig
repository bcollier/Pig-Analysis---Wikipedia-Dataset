/* script for social network */

/**********************************************************************************************************/
/* reduce to talk pages */

revsNom = LOAD '/data/CMU/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment:chararray, rev_user:int, rev_user_text:chararray, rev_timestamp:chararray, rev_minor_edit:int, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect, page_is_new , page_random, page_touched, page_latest, page_len);

revsNomSmall =  FOREACH revsNom GENERATE rev_id, rev_text_id, rev_user_text, rev_timestamp, page_title, page_namespace;

/* userPage = 2, userTalkPage = 3 */
revsNomUserTalk = FILTER revsNomSmall BY (page_namespace == 3);

/**********************************************************************************************************/
/* combine and reduce vot info */

voteTime = LOAD '/data/CMU/hcii/wikipedia/20080312/timeData' USING PigStorage('\t') AS (prospName:chararray, voterName:chararray, vote:int, successNum:int, year:int, month:int, day:int, hour:int, minute:int, utc:long, numSupBef:int, numNeutBef:int, numOppBef:int, numSup:int, numNeut:int, numOpp:int);

VAData = LOAD '/data/CMU/hcii/wikipedia/20080312/VAData' USING PigStorage('\t') AS (SupOpN:int, VoterID0:int, rfaid0:int, C_Username:chararray, V_Username:chararray, AdminAtVote, VoterID1, censor, comment, twiceinonemonth, badcontribdata, datename, username, alias, rfamonth, rfayear, rfamonthnum, rfa_yearmonth, monthssincefirstedit, editspermonth, attemptnum, totaledits, minoredits, articleedits, atalkedits, useredits, utalkedits, wpedits, wptalkedits, pctminoredits, pctarticleedits, pctatalkedits, pctutalkedits, pctwpedits, pctwptalkedits, pctanytalkedits, pctwporwptalk, pctuseredits, tenpctdiversity, twentypctdiversity, twentyfivepctdiversity, fivehundiversity, hundreddiversity, autocomments, totalComments, humanWrittenComments, pctwrittencomments, pctcommented, newLengthInWords, LengthInChars, log2commentlength, UniqueWords, please, thanks, vandal, revert, pov, pagescreated, pctunique, pctplease, pctthanks, pctvandal, pctrevert, pctpov, hasplease, hasthanks, hasvandal, hasrevert, haspov, firstedit, firstedityear, firsteditmonth, xfd, deletionall, rfc, otherrfas, vpump, votes, aiv, rfprotection, noticeboard, wikiquette, welcome, adminattn, arb, mediation, wikiproject, arb_or_mediation, CD, success, successNum, rfaid1);

VADataSmall = FOREACH VAData GENERATE CONCAT(C_Username, V_Username) AS cv, C_Username, V_Username, SupOpN, successNum,  rfayear, rfamonthnum, rfaid0, AdminAtVote, totaledits;
voteTimeSmall = FOREACH voteTime GENERATE CONCAT(prospName, voterName) AS cv, prospName, voterName, vote, successNum, year, month, day, hour, minute;

combine = JOIN VADataSmall BY cv, voteTimeSmall BY cv;
X = SAMPLE combine .01;
DUMP X;

test = FILTER combine BY C_Username != prospName OR V_Username != voterName OR SupOpN != vote OR VADataSmall::successNum != voteTimeSmall::successNum OR rfayear != year OR rfamonthnum != month;
DUMP TEST;

STORE = GROUP test

/**********************************************************************************************************/

/* only want to include the voters and the prospectives */
join1 = JOIN revsNomUserTalk BY rev_user_text, VADataSmall BY C_Username;
join2 = JOIN revsNomUserTalk BY rev_user_text, VADataSmall BY V_Username;
join3 = UNION join1, join2;
net = DISTINCT join3;

/* only relevent that rev_user_text posted on page_title userTalkPage */
netStore = FOREACH net GENERATE rev_id, rev_user_text, page_title, rev_timestamp, rfaDate;

netFilt = FILTER netStart BY rev_timestamp < rfaDate;
netTot = DISTINCT netFilter;
netStoreSmall = FOREACH netTot GENERATE rev_id, rev_user_text, page_title;

/* editor1, editor2, talkPageEditsEditor1MadeToEditor2 */
talkPageEditsGroup = GROUP netStoreSmall BY (rev_user_text, page_title);
talkPageEdits = FOREACH talkPageEditsGroup GENERATE FLATTEN(group), COUNT(netStoreSmall);

/* editor, totalTalkPageEditsEditorHas */
totalHasGroup = GROUP netStoreSmall BY (page_title)
totalHas = FOREACH totalHasGroup GENERATE FLATTEN(group), COUNT(netStoreSmall);

/* editor, totalTalkPageEditsEditorHasMade */
totalHasMadeGroup = GROUP netStoreSmall BY (rev_user_text)
totalHasMade = FOREACH totalHasMadeGroup GENERATE FLATTEN(group), COUNT(netStoreSmall);

/* editor1, editor2, totalTalkPageEditsBetweenBoth */
totalTalkCross = CROSS talkPageEdits, talkPageEdits;

/* rev_user_textA == page_titleB OR page_titleA = rev_user_textB */
totalTalkFilt = FILTER totalTalkCross BY ($0 == $4 OR $1 == $3);
totalTalk = FOREACH totalTalkFilt GENERATE $0, $1, ($2 + $5);

totalTalkThresh = FILTER totalTalk BY $2 > 10;

/**********************************************************************************************************/
/* run */

pig -Dmapred.job.queue.name=m45

/**********************************************************************************************************/
/* get number talk posts name1 to name2 */

revsNom = LOAD '/data/CMU/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment:chararray, rev_user:int, rev_user_text:chararray, rev_timestamp:chararray, rev_minor_edit:int, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect, page_is_new , page_random, page_touched, page_latest, page_len);

/* shrink */
revsNomSmall = FOREACH revsNom GENERATE rev_timestamp, rev_user_text, page_title, page_namespace, rev_id;

/* userPage = 2, userTalkPage = 3 */
revsNomUserTalk = FILTER revsNomSmall BY (page_namespace == 2 OR page_namespace == 3);

/**********************************************************************************************************/
/* full */

/* out */
revsNomSmall = FOREACH revsNomUserTalk GENERATE rev_timestamp, rev_user_text, page_title, rev_id;

STORE revsNomJoin INTO '/data/CMU/hcii/wikipedia/20080312/talkPages';
copyToLocal /data/CMU/hcii/wikipedia/20080312/talkPages talkPages;

/**********************************************************************************************************/
/* shrink */

VAData = LOAD '/data/CMU/hcii/wikipedia/20080312/VAData' USING PigStorage('\t') AS (SupOpN:int, VoterID0:int, rfaid0:int, C_Username:chararray, V_Username:chararray, AdminAtVote, VoterID1, censor, comment, twiceinonemonth, badcontribdata, datename, username, alias, rfamonth, rfayear, rfamonthnum, rfa_yearmonth, monthssincefirstedit, editspermonth, attemptnum, totaledits, minoredits, articleedits, atalkedits, useredits, utalkedits, wpedits, wptalkedits, pctminoredits, pctarticleedits, pctatalkedits, pctutalkedits, pctwpedits, pctwptalkedits, pctanytalkedits, pctwporwptalk, pctuseredits, tenpctdiversity, twentypctdiversity, twentyfivepctdiversity, fivehundiversity, hundreddiversity, autocomments, totalComments, humanWrittenComments, pctwrittencomments, pctcommented, newLengthInWords, LengthInChars, log2commentlength, UniqueWords, please, thanks, vandal, revert, pov, pagescreated, pctunique, pctplease, pctthanks, pctvandal, pctrevert, pctpov, hasplease, hasthanks, hasvandal, hasrevert, haspov, firstedit, firstedityear, firsteditmonth, xfd, deletionall, rfc, otherrfas, vpump, votes, aiv, rfprotection, noticeboard, wikiquette, welcome, adminattn, arb, mediation, wikiproject, arb_or_mediation, CD, success, successNum, rfaid1);

VAProsp = FOREACH VAData GENERATE C_Username;
VAVoter = FOREACH VAData GENERATE V_Username;
VAUnion = UNION VAProsp, VAVoter;
VADist = DISTINCT VAUnion;

/* out */
revsNomSmall = FOREACH revsNomUserTalk GENERATE rev_timestamp, rev_user_text, page_title, rev_id;

revsNomJoin1 = JOIN VADist BY $0, revsNomSmall BY rev_user_text;
revsNomJoin2 = JOIN VADist BY $0, revsNomSmall BY page_title;
revsNomJoin = UNION revsNomJoin1, revsNomJoin2;
revsNomFor = FOREACH revsNomJoin GENERATE rev_timestamp, rev_user_text, page_title, rev_id;

revsNomDist = DISTINCT revsNomFor;

STORE revsNomDist INTO '/data/CMU/hcii/wikipedia/20080312/talkPagesShrink';
copyToLocal /data/CMU/hcii/wikipedia/20080312/talkPagesShrink talkPagesShrink;

show = LOAD '/data/CMU/hcii/wikipedia/20080312/talkPagesShrink' USING PigStorage('\t');
DUMP show;

/**********************************************************************************************************/
/* prospects */

VAData = LOAD '/data/CMU/hcii/wikipedia/20080312/VAData' USING PigStorage('\t') AS (SupOpN:int, VoterID0:int, rfaid0:int, C_Username:chararray, V_Username:chararray, AdminAtVote, VoterID1, censor, comment, twiceinonemonth, badcontribdata, datename, username, alias, rfamonth, rfayear, rfamonthnum, rfa_yearmonth, monthssincefirstedit, editspermonth, attemptnum, totaledits, minoredits, articleedits, atalkedits, useredits, utalkedits, wpedits, wptalkedits, pctminoredits, pctarticleedits, pctatalkedits, pctutalkedits, pctwpedits, pctwptalkedits, pctanytalkedits, pctwporwptalk, pctuseredits, tenpctdiversity, twentypctdiversity, twentyfivepctdiversity, fivehundiversity, hundreddiversity, autocomments, totalComments, humanWrittenComments, pctwrittencomments, pctcommented, newLengthInWords, LengthInChars, log2commentlength, UniqueWords, please, thanks, vandal, revert, pov, pagescreated, pctunique, pctplease, pctthanks, pctvandal, pctrevert, pctpov, hasplease, hasthanks, hasvandal, hasrevert, haspov, firstedit, firstedityear, firsteditmonth, xfd, deletionall, rfc, otherrfas, vpump, votes, aiv, rfprotection, noticeboard, wikiquette, welcome, adminattn, arb, mediation, wikiproject, arb_or_mediation, CD, success, successNum, rfaid1);

VADataSmall = FOREACH VAData GENERATE rfa_yearmonth, C_Username, successNum;
VADataFilt = FILTER VADataSmall BY (successNum IS NOT NULL) AND (rfa_yearmonth IS NOT NULL) AND (C_Username IS NOT NULL);

VADist = DISTINCT VADataFilt;

STORE VADist INTO '/data/CMU/hcii/wikipedia/20080312/prospects';
copyToLocal /data/CMU/hcii/wikipedia/20080312/prospects prospects;





