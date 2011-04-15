ssh bcollier@gritgw.yahooresearchcluster.com
PW:  [normal password]


hadoop dfs -ls 

hadoop dfs -ls /data/CMU/hcii/wikipedia/20080312

hadoop dfs -ls /data/CMU/hcii/wikipedia/20080103


20080103/
uncompressed


logout	








----------------------------------------------
If you want just info on templates, see

hadoop dfs /data/CMU/hcii/wikipedia/20080103/templates.firstused


If you want full text, see
data/CMU/hcii/wikipedia/20080103/parsed.gzipped.repartitioned.uncompressed (or similar files, but I think that's the right one)
