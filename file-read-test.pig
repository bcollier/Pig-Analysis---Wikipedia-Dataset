rev = LOAD '/data/CMU/hcii/wikipedia/20110317/stubrevhead.txt' USING PigStorage('\t') AS (rev_id:int,page_id:int,revision_comment:chararray,rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);
rev2 = LIMIT rev 11;
STORE rev2 INTO '/data/CMU/hcii/wikipedia/20110317/rev11.txt' USING PigStorage('\t');

/*DUMP rev2;*/