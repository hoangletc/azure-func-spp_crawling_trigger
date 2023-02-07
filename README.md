# azure-func-spp_crawling_trigger

## 1.Input

Note that every inputs must follow OSLC format.

```
{
    'res': 'inventory' | 'all',
    'orderby' : '+matuse' | null,
    'where' : 'trans>="2021-01-01"' | null,
}
```