/*

Get all revisions for rfa candidates

MODIFIED:4-12-2011  

/*
revision file format from mwdumper
	rev_id:int
	page_id:int
	revision_comment:chararray
	rev_user_name:chararray
	rev_timestamp:chararray
	minor_bool:int
	rev_deleted:int
	rev_text"chararray
*/

/* This takes the join of revisions and pages and filters out all the revisions not from a nominee */

revsUnfilt = LOAD '/data/CMU/hcii/wikipedia/20110317/stub-meta-history_revisions.txt' USING PigStorage('\t') AS (rev_id:int,page_id:int,revision_comment:chararray,rev_user_id:int, rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);

--revsUnfilt = LOAD '/data/CMU/hcii/wikipedia/20110317/stubrevhead.txt' USING PigStorage('\t') AS (rev_id:int,page_id:int,revision_comment:chararray,rev_user_id:int, rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);

/* get users */
usersToLookAt = LOAD '/data/CMU/hcii/wikipedia/20110317/candidate_alias_names.txt' USING PigStorage('\t') AS (username:chararray, alias:chararray);
--usersToLookAt = LOAD '/data/CMU/hcii/wikipedia/20110317/userjointest.txt' USING PigStorage('\t') AS (username_reformat:chararray);
unique_users = DISTINCT usersToLookAt;


revs = JOIN unique_users BY username, revsUnfilt BY rev_user_text;


STORE revs INTO '/data/CMU/hcii/wikipedia/20110317/alias_revisions.out' USING PigStorage('\t');