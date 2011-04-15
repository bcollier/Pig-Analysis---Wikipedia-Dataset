rev = LOAD '/data/CMU/hcii/wikipedia/20110317/stubrevhead.txt' USING PigStorage('\t') AS (rev_id:long,page_id:long,revision_comment:chararray,rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);
rev2 = LIMIT rev 10;
DUMP rev2;
scriptDone;