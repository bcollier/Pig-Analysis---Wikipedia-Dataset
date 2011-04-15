/* these scripts are for actually producing number of revs, talk pages, pages, etc. for each month */

revsNom = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonth' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);

pagesGroup = GROUP revsNom ALL;
ans = FOREACH pagesGroup GENERATE COUNT(*);
DUMP ans;

revsNom = LOAD '/projects/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);

pagesGroup = GROUP revsNom ALL;
ans = FOREACH pagesGroup GENERATE COUNT(*);
DUMP ans;

/************************************************************************************/

revsNom = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonth' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);

pagesGroup = GROUP revsNom ALL;
result = FOREACH pagesGroup GENERATE FLATTEN(revsNom);

STORE result INTO '/projects/hcii/wikipedia/20080312/revsYearMonthFile' USING PigStorage('\t');

/************************************************************************************/

revsNom = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthFile' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);

create = FOREACH revsNom GENERATE rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month;

STORE create INTO '/projects/hcii/wikipedia/20080312/revsYearMonthGroup' USING PigStorage('\t');

/************************************************************************************/

revsNom = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthGroup' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month, rev_group);

DUMP revsNom;

/* total revisions this user did in this year/month */
revsUserMonth = GROUP revsNom BY rev_group;

create = FOREACH revsUserMonth GENERATE group, COUNT(revsNom);
create = FOREACH revsUserMonth {user = DISTINCT revsNom.rev_user_text; year = DISTINCT revsNom.rev_year; month = DISTINCT revsNom.rev_month; GENERATE user, year, month, group, COUNT(revsNom);};
DUMP create;

STORE create INTO 'dbunker/revsUserMonth' USING PigStorage('\t');

/************************************************************************************/

/* ALTERNATE */
/* Just get the total number of edits for each year-month for everyone */

revsNom = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthFile' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);
DUMP revsNom;

/* total revisions this user did in this year/month */
revsUserMonth = GROUP revsNom BY (rev_user_text, rev_year, rev_month);

create = FOREACH revsUserMonth GENERATE group.rev_user_text, group.rev_year, group.rev_month, COUNT(revsNom);
DUMP create;

STORE create INTO 'dbunker/revsUserMonth' USING PigStorage('\t');

/* check create against create2 */
X = GROUP create ALL;
ans = FOREACH X GENERATE COUNT(*);
DUMP ans;
(52432L)

/************************************************************************************/

/* ALTERNATE */
/* get the total number of pages for each year-month for everyone */

revsNom = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthFile' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);
DUMP revsNom;

/* total revisions this user did on this page for this year/month */
revsUserMonth = GROUP revsNom BY (rev_user_text, page_title, rev_year, rev_month);

/* this will condense all edits of a page into one (represents page edited this month) */
condense = FOREACH revsUserMonth GENERATE group.rev_user_text, group.page_title, group.rev_year, group.rev_month, COUNT(revsNom);
DUMP condense;

pagesUserMonth = GROUP condense BY (rev_user_text, rev_year, rev_month);

create = FOREACH pagesUserMonth GENERATE group.rev_user_text, group.rev_year, group.rev_month, COUNT(condense);
DUMP create;

STORE create INTO 'dbunker/pagesUserMonth' USING PigStorage('\t');

/************************************************************************************/

/* ALTERNATE */
/* get the total number of user/usertalk edits for each year-month for everyone */

revsNomStart = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthFile' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);
DUMP revsNom;

/* we can make sure only right revisions are looked at by filtering to just user/usertalk namespaces 2 and 3 */
revsNom = FILTER revsNomStart BY (page_namespace == 2) OR (page_namespace == 3);

/* total revisions this user did in this year/month */
revsUserMonth = GROUP revsNom BY (rev_user_text, rev_year, rev_month);

create = FOREACH revsUserMonth GENERATE group.rev_user_text, group.rev_year, group.rev_month, COUNT(revsNom);
DUMP create;

STORE create INTO 'dbunker/revsUserTalkUserMonth' USING PigStorage('\t');

/************************************************************************************/

/* ALTERNATE */
/* get the total number of talk edits for each year-month for everyone */

revsNomStart = LOAD '/projects/hcii/wikipedia/20080312/revsYearMonthFile' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len, rev_year, rev_month);
DUMP revsNom;

/* we can make sure only right revisions are looked at by filtering to just plain talk namespaces 1 */
revsNom = FILTER revsNomStart BY (page_namespace == 1);

/* total revisions this user did in this year/month */
revsUserMonth = GROUP revsNom BY (rev_user_text, rev_year, rev_month);

create = FOREACH revsUserMonth GENERATE group.rev_user_text, group.rev_year, group.rev_month, COUNT(revsNom);
DUMP create;

STORE create INTO 'dbunker/revsTalkUserMonth' USING PigStorage('\t');

/************************************************************************************/

