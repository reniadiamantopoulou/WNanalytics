-- Better native approach!
-- percentile_disc(array[0.02,0.25,0.5,0.75,0.98]) WITHIN GROUP (ORDER BY transit_views)

CREATE OR REPLACE FUNCTION _final_boxplot(mylist float[])
   RETURNS float[] AS
$$
    mylist.sort()
    l = len(mylist) - 1
    perc = [0.02, 0.25, 0.5, 0.75, 0.98]
    ind = [int(round(p*l)) for p in perc]
    return [mylist[i] for i in ind]
$$
LANGUAGE 'plpythonu' IMMUTABLE;
 
CREATE AGGREGATE boxplot(float) (
  SFUNC=array_append,
  STYPE=float[],
  FINALFUNC=_final_boxplot
);


select active_day, boxplot(sessions_per_day)
from temp_stats
group by active_day;
 active_day |   boxplot    
------------+--------------
          8 | {1,1,1,3,9}
          4 | {1,1,1,3,8}
          1 | {1,1,1,2,8}
          5 | {1,1,1,3,12}
         11 | {1,1,1,1,1}
          3 | {1,1,1,2,12}
         12 | {2,2,2,2,2}
         10 | {1,1,1,2,3}
          9 | {1,1,1,1,5}
          6 | {1,1,1,2,9}
          2 | {1,1,1,2,9}
          7 | {1,1,1,2,7}


