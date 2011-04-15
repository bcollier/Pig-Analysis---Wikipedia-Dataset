revsUnfilt = LOAD '/home/bcollier/Data/WPDump/combined/stubrevhead.txt' USING PigStorage('\t') AS (rev_id:int,page_id:int,revision_comment:chararray,rev_user_id:int, rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);

/* get users */
usersToLookAt = LOAD '/home/bcollier/Data/WPDump/combined/userjointest.txt' USING PigStorage('\t') AS (username_reformat:chararray);

usersToLookAt2 = LOAD '/home/bcollier/Data/WPDump/combined/userjointest.txt' USING PigStorage('\t') AS (username_reformat:chararray);

/*AS (username:chararray, username_reformat:chararray, user_url:chararray, attemptnum:int, attemptdate:chararray, success:int);*/
/* we use join here in order to filter unwanted users (it will perform an inner join) */
/* join can act like a filter! */
revs = JOIN usersToLookAt BY username_reformat, revsUnfilt BY rev_user_text;

/*DUMP revs;*/

STORE revs INTO '/home/bcollier/Data/WPDump/combined/rfa_revisions.out' USING PigStorage('\t');

