# Various tools

Write large file: 

```
> pv yourfile.csv | http -S -h POST http://${URL}/upload/${UPLOAD_HANDLE} Content-Type:text/csv
> curl -d "@yourfile.csv" -X POST http://${URL}/upload/${UPLOAD_HANDLE} -H "Content-Type:text/csv"
```

Read large file:

```
> curl -XGET http://${URL}/data/${GSIM}/${VERSION} -H "Accept:text/csv" | pv > /dev/null
```

## Zeppelin

First, start zeppelin:
```
> docker run -p 8080:8080 -it apache/zeppelin:0.8.1
```

Create a tunnel to the spark master:

```
> gcloud compute ssh ssb-drill-a-m --project lds-sandkasse --zone europe-north1-a -- -L 172.17.0.1:7077:localhost:7077 -N -v
```

Configure the spark interpreter with `spark://172.17.0.1:7077`

Example query
```
%sql
CREATE TEMPORARY VIEW PersonWithIncome USING org.apache.spark.sql.parquet OPTIONS (
    path "gs://ssb-data-a/data/b9c10b86-5867-4270-b56e-ee7439fe381e/0169581f-8928-11f6-82fb-1d298e67ee62"
)
EXPLAIN EXTENDED SELECT 
    * 
FROM (
    SELECT avg(income) as avg_income, MUNICIPALITY 
    FROM PersonWithIncome 
    GROUP BY MUNICIPALITY 
    ORDER BY MUNICIPALITY DESC
) a 
INNER JOIN (
    SELECT sum(income) as sum_income, MUNICIPALITY 
    FROM PersonWithIncome 
    GROUP BY MUNICIPALITY 
    ORDER BY MUNICIPALITY DESC) s ON s.MUNICIPALITY == a.MUNICIPALITY LIMIT 10;
```