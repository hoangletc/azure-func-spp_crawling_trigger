

import argparse
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Literal

import aiohttp

logging.getLogger().setLevel(logging.INFO)

NETWORK_CONF = {
    'dev': {
        "ip": "10.100.60.130",
        "port":  9082,
        "key": "bWF4YWRtaW46MTIzNDU2",
        "user": "maxauth",
        "endpoint": "maxtest/oslc/os"
    },
    'prod': {
        "ip": "10.100.62.26",
        "port":  80,
        "key": "bWF4YWRtaW46U3B2YkAxMjM=",
        "user": "maxauth",
        "endpoint": "maximo/oslc/os"
    }
}


PATH_ROOT_DATA = Path("crawled")
PATH_RES_INFO = Path("res_info.json")
# Read res_name info
with open(PATH_RES_INFO) as fp:
    RES_INFO = json.load(fp)


async def test():
    await asyncio.sleep(0.001)
    return {'fdsfsd': 'fndskf'}


def get_urls(
    res_name: str,
    fields: List[str],
    orderby: str = None,
    where_con: str = None,
    scheme: str = 'http',
    ip: str = "10.100.62.26",
    port: int = 80,
    endpoint: str = "maximo/oslc/os",
    pagesize: int = 1000,
    start_page: int = 1,
    max_page: int = 200,
    lean: int = 1
) -> List[str]:

    out = []

    res_names = [res_name]
    selected = ','.join(fields) if fields != [] else None
    if res_name == 'all':
        res_names = RES_INFO.keys()
        selected = '*'

        # As selecting all, disregard order and where_con
        orderby = where_con = None

    for name in res_names:
        for pageno in range(start_page, max_page + 1):
            params = {
                'lean': lean,
                'pageno': pageno,
                'oslc.pageSize': pagesize,
                'collectioncount': 1,
                'ignorecollectionref': 1
            }

            if selected is not None:
                params['oslc.select'] = selected
            if orderby is not None:
                params['oslc.orderby'] = orderby
            if where_con is not None:
                params['oslc.where'] = where_con

            url = f"{scheme}://{ip}:{port}/{endpoint}/{name}/"

            out.append({
                'url': url,
                'params': params,
                'pageno': pageno
            })

    return out


def parse_args():
    parser = argparse.ArgumentParser(description='TCData crawler')
    parser.add_argument('--maxcon', '-n', type=int, default=8,
                        help='Max connections established to server')
    parser.add_argument('--profile', '-p', type=str, choices=['dev', 'prod'],
                        default="prod", help='Connection profile')
    parser.add_argument('--resource', '-r', type=str,
                        default="all", help='Crawled resource')
    parser.add_argument('--user', type=str, default=None, help='User')
    parser.add_argument('--key', type=str, default=None, help='Password')
    parser.add_argument('--ip', type=str, default=None, help='IP')
    parser.add_argument('--port', type=str, default=None, help='Port')
    parser.add_argument('--pagesize', type=int, default=1000, help='Page size')
    parser.add_argument('--pagenum', type=int, default=None, help='Page num')
    parser.add_argument('--startpage', type=int, default=1, help='Start page')
    parser.add_argument('--preprocess', action=argparse.BooleanOptionalAction, help='Page num')

    return parser.parse_args()


async def get_res(
    session,
    url_info,
    res_name: str,
    user: str,
    key: str,
    data_type: str = "raw"
):
    url, params = url_info['url'], url_info['params']
    headers = {user: key}

    logging.info(f"Crawling: {url} - {url_info['pageno']}")

    async with session.get(url, params=params, headers=headers) as resp:
        out = await resp.json(encoding='utf-8', content_type=False)

    # Save data
    if data_type == "raw":
        output = {res_name: out}
        save_output(output, url_info['pageno'], "raw")
    else:
        # TODO: HoangLe [Feb-08]: Implement later
        pass

    return {
        'data': out,
        'pagenum': url_info['pageno']
    }


def crawl(
    res_name: str,
    profile: dict,
    pagesize: int = 1000,
    start_page: int = 1,
    max_page: int = 200,
    max_conn: int = 10,
    data_type: str = "raw",
    endpoint: str = "maximo/oslc/os"
):
    async def _trigger(url_infos_total):
        output = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(url_infos_total), max_conn):
                url_infos = url_infos_total[i:i+max_conn]

                tasks = [
                    asyncio.ensure_future(get_res(session, url_info, res_name,
                                                  profile['user'], profile['key'], data_type))
                    for url_info in url_infos
                ]

                out = await asyncio.gather(*tasks)

                output.append(out)

        return output

    fields = RES_INFO[res_name]['fields']

    url_infos_total = get_urls(res_name, fields, ip=profile['ip'], port=profile['port'],
                               pagesize=pagesize, start_page=start_page, max_page=max_page,
                               endpoint=endpoint)

    output = asyncio.run(_trigger(url_infos_total))

    return output


def save_output(
    data: dict,
    pageno: int,
    dtype: Literal["raw", "processed"] = "raw"
):
    # Create filename
    a = datetime.now()
    dt_s = a.strftime("%Y%m%d-%H%M%S")

    filename = f"{res_name}_{pageno:04d}_{dt_s}.json"

    path_out: Path = PATH_ROOT_DATA / dtype / res_name / filename
    path_out.parent.mkdir(parents=True, exist_ok=True)

    path_out.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding='utf-8'
    )


if __name__ == '__main__':
    args = parse_args()

    profile = NETWORK_CONF[args.profile]

    profile['user'] = args.user if args.user is not None else profile['user']
    profile['key'] = args.key if args.key is not None else profile['key']
    profile['ip'] = args.ip if args.ip is not None else profile['ip']
    profile['port'] = args.port if args.port is not None else profile['port']

    # Start crawling
    start = datetime.now()
    ds = start.strftime(r"%Y-%m-%d %H:%M:%S")
    logging.info(f"Start crawling at: {ds}")

    res_names = list(RES_INFO.keys()) if args.resource == "all" else [args.resource]
    for res_name in res_names:
        logging.info(f"== Processing: {res_name}")

        path_out: Path = PATH_ROOT_DATA / "raw" / res_name

        max_page = args.pagenum if args.pagenum is not None \
            else RES_INFO[res_name]['pagenum']

        flag_not_enough = False
        if not path_out.exists():
            flag_not_enough = True

        elif len(list(path_out.glob("*.json"))) < max_page:
            flag_not_enough = True

        if flag_not_enough is True:
            start_page = len(list(path_out.glob("*.json"))) + 1
            dtype = "processed" if args.preprocess is True else "raw"

            crawl(res_name, profile, args.pagesize,
                  start_page, max_page, args.maxcon, endpoint=profile['endpoint'])

    # Stop crawling
    stop = datetime.now()
    ds = stop.strftime(r"%Y-%m-%d %H:%M:%S")
    logging.info(f"Stop crawling at: {ds}")

    ds = (stop - start).strftime(r"%Y-%m-%d %H:%M:%S")
    logging.info(f"== Total crawling time: {ds}")


# Test one res:         python crawling.py -r BI_INVT --pagesize 1 --pagenum 1
# Test multiple res:    python crawling.py -r all --pagesize 1 --pagenum 1
# Run:                  python crawling.py
