/* this script is designed to get the network information about which users interact most often */

/**********************************************************************************************************/
/* reduce to talk pages */

revsNom = LOAD '/data/CMU/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment:chararray, rev_user:int, rev_user_text:chararray, rev_timestamp:chararray, rev_minor_edit:int, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect, page_is_new , page_random, page_touched, page_latest, page_len);

revsNomSmall = FOREACH revsNom GENERATE rev_user_text, page_title, rev_timestamp, rev_id, page_namespace;

/* userPage = 2, userTalkPage = 3, filter to userTalkPage */
revsNomUserTalk = FILTER revsNomSmall BY page_namespace == 3;

/* only use 20070500000000 to 20070700000000, ascii order of int is same as decimal order */
revsNomDate = FILTER revsNomUserTalk BY '20070500000000' <= rev_timestamp AND rev_timestamp <= '20070700000000';

/**********************************************************************************************************/
/* reduce voter vote info */

VAData = LOAD '/data/CMU/hcii/wikipedia/20080312/VAData' USING PigStorage('\t') AS (SupOpN:int, VoterID0:int, rfaid0:int, C_Username:chararray, V_Username:chararray, AdminAtVote, VoterID1, censor, comment, twiceinonemonth, badcontribdata, datename, username, alias, rfamonth, rfayear, rfamonthnum, rfa_yearmonth:chararray, monthssincefirstedit, editspermonth, attemptnum, totaledits, minoredits, articleedits, atalkedits, useredits, utalkedits, wpedits, wptalkedits, pctminoredits, pctarticleedits, pctatalkedits, pctutalkedits, pctwpedits, pctwptalkedits, pctanytalkedits, pctwporwptalk, pctuseredits, tenpctdiversity, twentypctdiversity, twentyfivepctdiversity, fivehundiversity, hundreddiversity, autocomments, totalComments, humanWrittenComments, pctwrittencomments, pctcommented, newLengthInWords, LengthInChars, log2commentlength, UniqueWords, please, thanks, vandal, revert, pov, pagescreated, pctunique, pctplease, pctthanks, pctvandal, pctrevert, pctpov, hasplease, hasthanks, hasvandal, hasrevert, haspov, firstedit, firstedityear, firsteditmonth, xfd, deletionall, rfc, otherrfas, vpump, votes, aiv, rfprotection, noticeboard, wikiquette, welcome, adminattn, arb, mediation, wikiproject, arb_or_mediation, CD, success, successNum, rfaid1);

VADataSmall = FOREACH VAData GENERATE C_Username, V_Username, SupOpN, successNum, rfa_yearmonth, AdminAtVote;

/* only use 200707 */
VADataDate = FILTER VADataSmall BY rfa_yearmonth == '200707';
STORE VADataDate INTO '/data/CMU/hcii/wikipedia/20080312/VA07' USING PigStorage('\t');

/* reload */
VADataDate = LOAD '/data/CMU/hcii/wikipedia/20080312/VA07' USING PigStorage('\t') AS (C_Username, V_Username, SupOpN, successNum, rfa_yearmonth);

/* success or not */
VADataDateCand = FOREACH VADataDate GENERATE C_Username, successNum;
cand = DISTINCT VADataDateCand;
DUMP cand;

/* vote type */
VADataDateVoteUn = FOREACH VADataDate GENERATE V_Username, C_Username, SupOpN;
VADataDateVote = FILTER VADataDateVoteUn BY SupOpN == 1 OR SupOpN == -1;
vote = DISTINCT VADataDateVote;
STORE vote INTO '/data/CMU/hcii/wikipedia/20080312/VA07Sup' USING PigStorage('\t');

/* is admin */
isAdmin = FOREACH VADataDate GENERATE V_Username, AdminAtVote;
isAdminDist = DISTINCT isAdmin;
STORE isAdminDist INTO '/data/CMU/hcii/wikipedia/20080312/VA07Admin' USING PigStorage('\t');

/**********************************************************************************************************/
/* continue filter */

/* examined subset */
desiredUsers1 = FOREACH VADataDate GENERATE C_Username;
desiredUsers2 = FOREACH VADataDate GENERATE V_Username;
desiredUsersUnion = UNION desiredUsers1, desiredUsers2;
desiredUsers = DISTINCT desiredUsersUnion;

/* only want to include the voters and the prospectives */
/* filter rev_user_text to just desiredUsers and page_title (users page) to just desiredUsers */
join1 = JOIN revsNomDate BY rev_user_text, desiredUsers BY $0;
join2 = JOIN join1 BY page_title, desiredUsers BY $0;
net = FOREACH join2 GENERATE rev_user_text, page_title, rev_id;
netStoreSmall = DISTINCT net;

/* editor1, editor2, talkPageEditsEditor1MadeToEditor2 */
talkPageEditsGroup = GROUP netStoreSmall BY (rev_user_text, page_title);
talkPageEdits = FOREACH talkPageEditsGroup GENERATE FLATTEN(group), COUNT(netStoreSmall);

STORE talkPageEdits INTO '/data/CMU/hcii/wikipedia/20080312/talkPageEdits' USING PigStorage('\t');

/**********************************************************************************************************/
/* get talk page edits between the two unique editors */

talkPageEditsA = LOAD '/data/CMU/hcii/wikipedia/20080312/talkPageEdits' USING PigStorage('\t') AS (ed1A, ed2A, ed1ToEd2A);
talkPageEditsB = LOAD '/data/CMU/hcii/wikipedia/20080312/talkPageEdits' USING PigStorage('\t') AS (ed1B, ed2B, ed1ToEd2B);

/* editor1, editor2, totalTalkPageEditsBetweenBoth */
totalTalkCross = CROSS talkPageEditsA, talkPageEditsB;

/* rev_user_textA == page_titleB AND page_titleA = rev_user_textB AND can't have both ed1A,ed2A and ed2A,ed1A (redundant) */
totalTalkFilt = FILTER totalTalkCross BY (ed1A == ed2B AND ed1B == ed2A AND ed1A < ed2A);
totalTalk = FOREACH totalTalkFilt GENERATE ed1A, ed2A, (ed1ToEd2A + ed1ToEd2B);

STORE totalTalk INTO '/data/CMU/hcii/wikipedia/20080312/totalTalk' USING PigStorage('\t');
/**********************************************************************************************************/

totalTalk = LOAD '/data/CMU/hcii/wikipedia/20080312/totalTalk' USING PigStorage('\t') AS (ed1,ed2,editsBetween);

desiredUsers1 = FOREACH totalTalk GENERATE ed1;
desiredUsers2 = FOREACH totalTalk GENERATE ed2;
desiredUsersUnion = UNION desiredUsers1, desiredUsers2;
desiredUsers = DISTINCT desiredUsersUnion;

STORE desiredUsers INTO '/data/CMU/hcii/wikipedia/20080312/usersTalk' USING PigStorage('\t');



