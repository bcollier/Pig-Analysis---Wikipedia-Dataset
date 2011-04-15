revsUnfilt = LOAD '/data/CMU/hcii/wikipedia/20110317/rfa_revisions.out.cat' USING PigStorage('\t') AS (rfa_username:chararray,rev_id:int,page_id:int,revision_comment:chararray,rev_user_id:int, rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);
--revsUnfilt = LOAD '/data/CMU/hcii/wikipedia/20110317/stubrevhead.txt' USING PigStorage('\t') AS (rev_id:int,page_id:int,revision_comment:chararray,rev_user_id:int, rev_user_text:chararray,rev_timestamp:chararray,minor_bool:int,rev_deleted:int,rev_text:chararray);

usergroup = GROUP revsUnfilt by rev_user_text;

userrevcounts = FOREACH usergroup GENERATE group, COUNT(revsUnfilt) as rev_count;

--userrevcountsFiltered = FILTER userrevcounts BY rev_count > 10;

STORE userrevcounts INTO '/data/CMU/hcii/wikipedia/20110317/rfa_revisions_overall_count.out' USING PigStorage('\t');
