Write large file: 

```
pv yourfile.csv | http -S -h POST http://${URL}/upload/${UPLOAD_HANDLE} Content-Type:text/csv
curl -d "@yourfile.csv" -X POST http://${URL}/upload/${UPLOAD_HANDLE} -H "Content-Type:text/csv"
```

Read large file:

```
curl -XGET http://${URL}/data/${GSIM}/${VERSION} -H "Accept:text/csv" | pv > /dev/null
```