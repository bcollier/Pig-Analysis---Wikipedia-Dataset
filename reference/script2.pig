/* this is for getting the peices out of rev_timestamp, pig isn't really built for this so i had to use a bunch of filters */

revsNom = LOAD '/projects/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment:chararray, rev_user:int, rev_user_text:chararray, rev_timestamp:chararray, rev_minor_edit:int, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len);

/***********************************************************************************************************************************************************/

result10 = FILTER revsNom BY (rev_timestamp matches '^2001.*');
result11 = FOREACH result10 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2001;

result20 = FILTER revsNom BY (rev_timestamp matches '^2002.*');
result21 = FOREACH result20 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2002;
revsYearTemp = UNION revsYearTemp, result1;

result30 = FILTER revsNom BY (rev_timestamp matches '^2003.*');
result31 = FOREACH result30 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2003;

result40 = FILTER revsNom BY (rev_timestamp matches '^2004.*');
result41 = FOREACH result40 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2004;

result50 = FILTER revsNom BY (rev_timestamp matches '^2005.*');
result51 = FOREACH result50 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2005;

result60 = FILTER revsNom BY (rev_timestamp matches '^2006.*');
result61 = FOREACH result60 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2006;

result70 = FILTER revsNom BY (rev_timestamp matches '^2007.*');
result71 = FOREACH result70 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2007;

result80 = FILTER revsNom BY (rev_timestamp matches '^2008.*');
result81 = FOREACH result80 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2008;

result90 = FILTER revsNom BY (rev_timestamp matches '^2009.*');
result91 = FOREACH result90 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, 2009;

revsYearTemp = UNION result11, result21, result31, result41, result51, result61, result71, result81, result91;
STORE revsYearTemp INTO '/projects/hcii/wikipedia/20080312/revsYearTemp' USING PigStorage('\t');

/***********************************************************************************************************************************************************/

X = GROUP revsNom BY (rev_user_text, page_title);
Y = FOREACH X GENERATE group, revsNom.rev_user_text, revsNom.page_title;
Z = FOREACH Y FLATTEN(group);

/***********************************************************************************************************************************************************/

revsNomYear = LOAD '/projects/hcii/wikipedia/20080312/revsYearTemp' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year);

resultMon010 = FILTER revsNomYear BY (rev_timestamp matches '^....01.*');
resultMon011 = FOREACH resultMon010 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 1;

resultMon020 = FILTER revsNomYear BY (rev_timestamp matches '^....02.*');
resultMon021 = FOREACH resultMon020 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 2;

resultMon030 = FILTER revsNomYear BY (rev_timestamp matches '^....03.*');
resultMon031 = FOREACH resultMon030 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 3;

resultMon040 = FILTER revsNomYear BY (rev_timestamp matches '^....04.*');
resultMon041 = FOREACH resultMon040 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 4;

resultMon050 = FILTER revsNomYear BY (rev_timestamp matches '^....05.*');
resultMon051 = FOREACH resultMon050 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 5;

resultMon060 = FILTER revsNomYear BY (rev_timestamp matches '^....06.*');
resultMon061 = FOREACH resultMon060 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 6;

resultMon070 = FILTER revsNomYear BY (rev_timestamp matches '^....07.*');
resultMon071 = FOREACH resultMon070 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 7;

resultMon080 = FILTER revsNomYear BY (rev_timestamp matches '^....08.*');
resultMon081 = FOREACH resultMon080 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 8;

resultMon090 = FILTER revsNomYear BY (rev_timestamp matches '^....09.*');
resultMon091 = FOREACH resultMon090 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 9;

resultMon100 = FILTER revsNomYear BY (rev_timestamp matches '^....10.*');
resultMon101 = FOREACH resultMon100 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 10;

resultMon110 = FILTER revsNomYear BY (rev_timestamp matches '^....11.*');
resultMon111 = FOREACH resultMon110 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 11;

resultMon120 = FILTER revsNomYear BY (rev_timestamp matches '^....12.*');
resultMon121 = FOREACH resultMon120 GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, 12;

revsYearMonth = UNION resultMon011, resultMon021, resultMon031, resultMon041, resultMon051, resultMon061, resultMon071, resultMon081, resultMon091, resultMon101, resultMon111, resultMon121;
STORE revsYearMonth INTO '/projects/hcii/wikipedia/20080312/revsYearMonth' USING PigStorage('\t');

/***********************************************************************************************************************************************************/

revsNomYearMonth = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthHold2' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);

DUMP revsNomYearMonth;


/***********************************************************************************************************************************************************/


