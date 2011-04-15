/* this script is for grouping by the pages people edit, this ultimately did not work, but i saved the script */

Script

/* This takes the join of revisions and pages and filters out all the revisions not from a nominee */

revsUnfilt = LOAD '/projects/hcii/wikipedia/20080312/revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len);

/* get users */
usersToLookAt = LOAD '/user/moira/dbunker/noms' USING PigStorage('\t') AS (nom);

/* we use join here in order to filter unwanted users (it will perform an inner join) */
/* join can act like a filter! */
revs = JOIN revsUnfilt BY rev_user_text, usersToLookAt BY nom;
DUMP revs;

/* example user of flatten */
/* revs = FOREACH revsNomUsers GENERATE FLATTEN(revsUnfilt); */

STORE revs INTO '/projects/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t');

/**********************************************************************************/

/* this part is for getting the pages with the most nominee revs */

revsNom = LOAD '/projects/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len);

pageGroup = GROUP revsNom BY (page_title,page_id);

sizeInfo = FOREACH pageGroup GENERATE group.page_title,group.page_id,COUNT(revsNom);
DUMP sizeInfo;

result = FILTER sizeInfo BY ($2 > 50);
DUMP result;

STORE result INTO 'dbunker/nomNumPage' USING PigStorage('\t');

/***************************************************************************************/

/* gets the number of pages */

pages = LOAD 'dbunker/nomNumPage' USING PigStorage('\t') AS (name,count,id);
pagesGroup = GROUP pages ALL;
ans = FOREACH pagesGroup GENERATE COUNT(*);
DUMP ans;

look = FILTER pages BY (count > 1000);
DUMP look;

/***************************************************************************************/

/* Create random batch of pages from the about 60000 pages started with */

pages = LOAD 'dbunker/nomNumPage' USING PigStorage('\t') AS (name,id:long,count);
pagesWithRand = FOREACH pages GENERATE name,id,count,((((((((id*1103515245+392349)%574531)*1103515245+392349)%574531)*1103515245+392349)%574531)*1103515245+392349)%574531);

/* pseudo random number generator */
((id*1103515245+392349)%574531)

/* get a portion */
574531/10 = 57453

result1 = FILTER pagesWithRand BY ($3 < 57453);
DUMP result1;

result2 = FOREACH result1 GENERATE name,id,count;
DUMP result2;
STORE result2 INTO 'dbunker/nomNumPageRand' USING PigStorage('\t');

/***************************************************************************************/

/* get new number of pages */

pagesGroup = GROUP result2 ALL;
ans = FOREACH pagesGroup GENERATE COUNT(*);
DUMP ans;

/***************************************************************************************/

pages = LOAD 'dbunker/nomNumPageRand' USING PigStorage('\t') AS (name,count,id);
DUMP pages;

/***************************************************************************************/

copyToLocal dbunker/nomNumPageRand dbunker
nomNumPageRand

/***************************************************************************************/

/* now we actually get the filler for the matrix (each cell is person edited page X times) */

revsNom = LOAD '/projects/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id, rev_page, rev_text_id, rev_comment, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len);

pages = LOAD 'dbunker/nomNumPageRand' USING PigStorage('\t') AS (name,count,id);

/* we remove irrelevent stuff from both */
revsNom2 = FOREACH revsNom GENERATE rev_id,rev_user,rev_user_text,page_title;
pages2 = FOREACH pages GENERATE name,id,count;

/* restrict the nomenee revisions to the selected pages by joining them by page title*/
restrict = JOIN revsNom2 BY page_title,pages2 BY name;
DUMP restrict;

STORE restrict INTO 'dbunker/flatMat' USING PigStorage('\t');
copyToLocal dbunker/flatMat dbunker

/***************************************************************************************/

dbunker/flatMatFile
dbunker/myPages
/home/moira

/***************************************************************************************/

revsNom = LOAD '/projects/hcii/wikipedia/20080312/nominee_revision_join_page' USING PigStorage('\t') AS (rev_id:int, rev_page:int, rev_text_id:int, rev_comment:chararray, rev_user, rev_user_text, rev_timestamp, rev_minor_edit, rev_deleted, rev_len, rev_parent_id, page_id, page_namespace, page_title, page_restrictions, page_counter, page_is_redirect , page_is_new , page_random, page_touched, page_latest, page_len);

revGroup = GROUP revsNom BY rev_user_text, page_title;

pageEdits = FOREACH revGroup GENERATE revsNom.rev_user_text, revsNom.page_title, COUNT(revsNom) AS (userName,pageName,numRev);
STORE pageEdits INTO 'dbunker/pageEdits' USING PigStorage('\t');

pageGroup = GROUP pageEdits BY pageName;

next = FOREACH pageGroup GENERATE CROSS(pageEdits,pageEdits);

/***************************************************************************************/
