pig script:
 
page = LOAD '/data/CMU/hcii/wikipedia/20080312/page.txt' USING PigStorage('\t')AS (page_id:int, page_namespace:int, page_title:chararray,page_restrictions:chararray, page_counter:int, page_is_redirect:int, page_is_new:int, page_random:double,page_touched:chararray,page_latest:int, page_len:int);
imageInfo = LOAD '/data/CMU/hcii/wikipedia/20080312/imageInfo' USING PigStorage('\t') AS (key, page_id_i:int, revisiontime:chararray, revisionid, editor, bytes:int, image, giver, givetime, description);
imageInfo_f = FILTER imageInfo BY (revisiontime!='00000000000000') AND (revisiontime!='99999999999999');
page_limit = FOREACH page GENERATE page_id, page_namespace, page_title;
join_image_page = JOIN page_limit BY page_id, imageInfo_f BY page_id_i PARALLEL 100;
join_f = FILTER join_image_page BY (page_namespace==2) OR (page_namespace==3);
groupj = GROUP join_f BY page_id PARALLEL 50;
C = FOREACH groupj{
        D = ORDER join_f BY revisiontime ASC;
        E = LIMIT D 1;
        GENERATE E;
}
join_out = FOREACH C GENERATE FLATTEN($0);
STORE join_out INTO '/data/CMU/hcii/wikipedia/20080312/barnstars' USING PigStorage('\t');
