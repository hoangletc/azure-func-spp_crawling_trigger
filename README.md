# spp_crawling_trigger

## 0. Introduction


## 1. Input

### 1.1. Mode: 'quantity_prober'


```
{
    "mode_funcapp": "crawling_config_generator",
    "mode_crawl": <from environment argument> | "historical",
    "signal": <from environment argument>,
    "where": <from environment argument>,
    "ip": <from environment argument>,
    "port": <from environment argument>,
    "path": <from environment argument>,
    
}
```

### 1.2. Mode: 'crawling_config_generator'
INPUT:
```
{
    "mode_funcapp": "crawling_config_generator",
    "mode_crawl": <from environment argument> | "historical",
    "signal": <from environment argument>
}
```

OUTPUT:

```
[
    {
        "api_name": <from PROBER> | "BI_INVE",
        "select": <from PROBER> | [
            "inventoryid",
            "changedate"
        ],
        "where": <from PROBER> | "changedate%222023-02-01%22",
        "pageno": 1 | increase to pagenum - retrieved from PROBER,
        "pagesize": <from PROBER> | 1,
    }
]
```

### 1.3. Mode: 'gen_blobpath'
This module lists out all paths to raw files.

```
{
    "mode_funcapp": "gen_blobpath",
    "environment": <from environment argument> | prod,
    "signal": <from environment argument>
}
```


### 1.4. Mode: 'processing'
This module reads file from BlobStorage from given blob path, processes and stores back to Blob.

```
{
    "mode_funcapp": "processing",
    "blob": <output from previous stage> | "cmms/raw/2023-05-15/BI_WO/page_100_17:50:37:91.json"
    "environment": <from environment argument> | prod,
    "signal": <from environment argument>
}
```

### 1.5. Mode: 'collect_maxrowstamp'
This module reads all rowstamp configuration files.

```
{
    "mode_funcapp": "collect_rowstamp",
    "environment": <from environment argument> | prod
}



## 2. List các API có thể crawl
```
[
    'BI_MATU',
    'BI_MATR',
    'BI_ITEM',
    'BI_ASSET',
    'BI_ASSETSTATUS',
    'BI_INVE',
    'BI_INVT',
    'BI_LOC',
    'BI_SERV',
    'BI_WO'
]
```