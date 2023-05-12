# spp_crawling_trigger

## 0. Introduction

## 1. Version

#### v1.2: <Future>
- Module 'prober' implicitly calls Web --> to reduce components in Function app

#### v1.1: (Developing)
- Attach Experimental module(Copy data module) to get over 4MB issue of Web module
- Combine all code (prober, url creator, preprocess) into one single function app
- Module probing can estimate pagesize with probing URL having pagesize = 1


#### v1.0: Apr 26

## 2. Input

### 2.1. Mode: 'quantity_prober'


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

### 2.2. Mode: 'crawling_config_generator'
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

### 2.3. Mode: 'processing'
This module reads file from BlobStorage, processes and stores back to Blob.

```
{
    "mode_funcapp": "processing",
    "signal": <from environment argument>
}
```


## 3. List các API có thể crawl
```
[
    BI_MATU,
    BI_MATR,
    BI_ITEM,
    BI_ASSET,
    BI_ASSETSTATUS,
    BI_INVE,
    BI_INVT,
    BI_LOC,
    BI_SERV,
    BI_WO,
    BI_WOSTATUS
]
```