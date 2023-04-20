import requests as rq
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import urllib3, socket
from urllib3.connection import HTTPConnection
HTTPConnection.default_socket_options = (HTTPConnection.default_socket_options + [
            (socket.SOL_SOCKET, socket.SO_SNDBUF, 10000000), #1MB in byte
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 10000000)
        ])
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import htmls

def get_brand_data(link):
    brands = pd.DataFrame()
    responce = rq.get(link).text
    soup = BeautifulSoup(responce, 'lxml')
    v = soup.find(htmls.base_brand_v[0], class_=htmls.base_brand_v[1])
    if v is None:
        print('base_brand_v tag is empty')
    else:
        s = v.find_all(htmls.base_brand_s[0], class_=htmls.base_brand_s[1])
        if s is None:
            print('base_brand_s tag is empty')
        else:
            for i in s:
                bs_lnk = i.find_all('a')
                for i in bs_lnk:
                    tex = i.text
                    lin = i.get('href')
                    brands = brands.append({'brand': tex, 'link': lin}, ignore_index=True)
            f = soup.find_all('noscript')
            for i in f:
                block = i.find_all('a')
                for link in block:
                    lnk = link.get('href')
                    txt = link.text
                    brands = brands.append({'brand': txt, 'link': lnk}, ignore_index=True)
    return brands

def get_model_data(brand_row):
    brand = brand_row[1]
    link = brand_row[2]

    models = pd.DataFrame(columns=['brand', 'model', 'link'])

    try:
        responce = rq.get(link).text
        soup = BeautifulSoup(responce, 'lxml')
    except:
        time.sleep(1)
        print(str(link) + ' no responce')

    f = soup.find_all(htmls.base_model_f[0], class_=htmls.base_model_f[1])
    if f is None:
        print('base_model_f tag is empty')
    else:
        for i in f:
            p = i.find_all(htmls.base_model_p[0], class_=htmls.base_model_p[1])
            if p is None:
                print('base_model_p tag is empty')
            else:
                for i in p:
                    lnk = i.get('href')
                    modelname = i.text
                    # print(modelname)
                    models = models.append({'brand': brand, 'model': modelname, 'link': lnk}, ignore_index=True)
    return models

def get_generations_data(model_row):
    generations = pd.DataFrame(columns=['brand', 'model','vehicle_region', 'model_name', 'generation',
                                        'body', 'status', 'link'])

    link = model_row[3]
    brand = model_row[1]
    model = model_row[2]
    try:
        responce = rq.get(link, timeout=2).text
        soup = BeautifulSoup(responce, 'lxml')

    except:
        time.sleep(1)
        print(str(link) + ' no responce')

    try:
        g = soup.find(htmls.base_generation_g[0], class_=htmls.base_generation_g[1])
    except:
        print("No coonection to this generation: " + link)
    try:
        m = g.find_all(htmls.base_generation_m[0], class_=htmls.base_generation_m[1])
        if m is None:
            print('base_generation_m is empty')
        else:
            for i in m:
                region = i.find(htmls.base_generation_region[0], class_=htmls.base_generation_region[1]).text
                md = i.find_all(htmls.base_generation_md[0], class_=htmls.base_generation_md[1])

                if region is None or md is None:
                    print('base_generation_region tag or base_generation_md tag is empty')
                else:
                    for i in md:

                        l = i.find(htmls.base_generation_l[0], class_=htmls.base_generation_l[1]).get('href')

                        if i.find(htmls.base_generation_model_name[0],
                                  class_=htmls.base_generation_model_name[1]) is None:
                            model_name = 'none'
                            print('base_generation_model_name tag is empty')
                        else:
                            model_name = i.find(htmls.base_generation_model_name[0],
                                                class_=htmls.base_generation_model_name[1]).text
                            # print(model_name)

                        sub_data = i.find_all(htmls.base_generation_sub_data[0],
                                              class_=htmls.base_generation_sub_data[1])
                        gen_name = sub_data[0].text
                        body_type = sub_data[1].text
                        try:
                            vehicle_status = i.find(htmls.base_generation_vehicle_status[0],
                                                    class_=htmls.base_generation_vehicle_status[1]).text
                        except:
                            vehicle_status = "inactive"

                        lnk = link + l
                        generations = generations.append({'brand': brand, 'model': model,
                                                          'vehicle_region': region, 'model_name': model_name,
                                                          'generation': gen_name,
                                                          'body': body_type, 'status': vehicle_status, 'link': lnk},
                                                         ignore_index=True)
    except:
        print("No coonection to this generation: " + link)

    generations['model_name'] = generations['model_name'].astype(str).str.strip()
    generations['model_name'] = generations['model_name'].str.replace(" ", "_")
    generations['model_name'] = generations['model_name'].str.replace(r'\n', '_', regex=True)
    generations['model_name'] = generations['model_name'].str.replace(r'\r', '_', regex=True)
    generations['model_name'] = generations['model_name'].str.replace('___', '_', regex=True)
    return generations

def get_spec_data(generation_row):
    spec = pd.DataFrame()

    link = generation_row[8]
    brnd = generation_row[1]
    mdl = generation_row[2]
    region = generation_row[3]
    mdl_nm = generation_row[4]
    gn = generation_row[5]
    bd = generation_row[6]
    sts = generation_row[7]
    try:
        responce = rq.get(link, timeout=2).text
    except:
        time.sleep(2)
        print("Spec response sucks with this one: " + link)
    try:
        soup = BeautifulSoup(responce, 'lxml')
        f = soup.find(htmls.base_spec_f[0], class_=htmls.base_spec_f[1])
        if f is None:
            print('base_spec_f tag is empty')
        else:
            try:
                d = f.find_all(htmls.base_spec_d[0], class_=htmls.base_spec_d[1])
                if d is None:
                    print('base_spec_d tag is empty')
                else:
                    for i in d:
                        lnk = i.a.get('href')
                        lnk = 'https://www.website.com' + lnk
                        check = 'Сравнить'
                        dt = i.find_all('td')
                        spec_name = dt[1].text
                        prod_dt = dt[2].text
                        price = dt[3].text
                        engine_code = dt[4].text
                        body_code = dt[5].text
                        if body_code == check:
                            body_code = dt[4].text
                            engine_code = dt[3].text
                            price = "n/a"
                        spec = spec.append(
                            {'brand': brnd, 'model': mdl, 'model_name': mdl_nm, 'vehicle_region': region,
                             'generation': gn,
                             'body': bd, 'status': sts, 'spec_name': spec_name, 'production_dates': prod_dt,
                             'price': price,
                             'engine_code': engine_code, 'body_code': body_code, 'link': lnk},
                            ignore_index=True)
                        spec['production_dates'] = spec['production_dates'].str.strip()
                        spec['production_dates'] = spec['production_dates'].str.replace(" ", "_")
                        spec['production_dates'] = spec['production_dates'].str.replace(r'\n', '_', regex=True)
                        spec['production_dates'] = spec['production_dates'].str.replace(r'\r', '_', regex=True)
                        spec['production_dates'] = spec['production_dates'].str.replace(
                            r'_____________________________',
                            '', regex=True)
                        spec['production_dates'] = spec['production_dates'].str.replace(
                            r'____________________________ - _____________________________________________________________',
                            '-', regex=True)
                        spec['production_dates'] = spec['production_dates'].str.replace(
                            r'____________________________', '',
                            regex=True)
                    counter = counter + 1
                    print("spec_complete %: " + str((counter / count) * 100))

            except:
                d = 'no_specs'
                spec = spec.append(
                    {'brand': brnd, 'model': mdl, 'model_name': mdl_nm, 'vehicle_region': region,
                     'generation': gn,
                     'body': bd, 'status': sts, 'spec_name': "No_spec", 'production_dates': "No_spec",
                     'price': "No_spec", 'engine_code': "No_spec", 'body_code': "No_spec", 'link': "No_spec"},
                    ignore_index=True)
                print("no specs " + brnd + " " + mdl_nm)
                counter = counter + 1
                print("spec_complete %: " + str((counter / count) * 100))
    except:
        time.sleep(1)
    return spec

def get_base_vehicle_data(spec_row):
    from requests.exceptions import Timeout, ConnectionError

    headers = {
        'User-Agent': htmls.header_mozila}
    global responce
    base_vehicle_data = pd.DataFrame()

    link = spec_row[13]
    brand = spec_row[1]
    model = spec_row[2]
    model_name = spec_row[3]
    region = spec_row[4]
    generation = spec_row[5]
    body = spec_row[6]
    status = spec_row[7]
    spec_name = spec_row[8]
    prod_dates = spec_row[9]
    price = spec_row[10]
    engine_code = spec_row[11]
    body_code = spec_row[12]

    if link == "no specs" or link == "No specs" or link == "No_spec" or link == "no_spec" or link == "none":
        base_vehicle_data = base_vehicle_data.append(
            {'link_id':link, 'brand': brand, 'model': model, 'model_name': model_name, 'vehicle_region': region,
             generation: 'generation', 'body': body, 'status': status, 'spec_name': spec_name,
             'prod_dates': prod_dates, 'price': price, 'engine_code': engine_code, 'body_code': body_code,
             'vehicle': "no specs", 'type': "no specs", 'spec': "no specs"}, ignore_index=True)
    else:
        try:
            r = rq.get(link)
            if r.status_code != 200:
                print('Ошибка:')
                print(r.status_code)
            else:
                responce = rq.get(link, headers=headers, timeout=1).text
                soup = BeautifulSoup(responce, 'lxml')
                # название автомобиля
                header = soup.find(htmls.base_base_data_header[0], class_=htmls.base_base_data_header[1]).text
                if header is None:
                    print('base_base_data_header tag is empty')
                else:

                    # собираем общие данные по машине
                    main = soup.find_all(htmls.base_base_data_main[0], class_=htmls.base_base_data_main[1])
                    for i in main:
                        tp = i.find_all(htmls.base_base_data_tp[0], class_=htmls.base_base_data_tp[1])
                        for i in tp:
                            cols = i.find(htmls.base_base_data_cols[0], class_=htmls.base_base_data_cols[1]).text
                            spec = i.find(htmls.base_base_data_spec[0], class_=htmls.base_base_data_spec[1]).text
                            base_vehicle_data = base_vehicle_data.append(
                                {'link_id':link, 'brand': brand, 'model': model, 'model_name': model_name, 'vehicle_region': region,
                                 'generation': generation, 'body': body, 'status': status, 'spec_name': spec_name,
                                 'prod_dates': prod_dates, 'price': price, 'engine_code': engine_code,
                                 'body_code': body_code, 'vehicle': header, 'type': cols, 'spec': spec},
                                ignore_index=True)
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.strip()
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.replace(" ", "")
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.replace(r'\n',
                                                                                                          ' ',
                                                                                                          regex=True)
                            base_vehicle_data['model_name'] = base_vehicle_data['model_name'].str.replace(r'\n',
                                                                                                          ' ',
                                                                                                          regex=True)
                            base_vehicle_data['model_name'] = base_vehicle_data['model_name'].str.replace("_x000D_",
                                                                                                          " ")
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.replace(
                                "____________________________ - _____________________________________________________________",
                                "-")
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.replace(
                                "_____________________________", "")
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.replace(
                                "____________________________", "")
                            base_vehicle_data['prod_dates'] = base_vehicle_data['prod_dates'].str.replace(
                                "___", "")
                            base_vehicle_data = base_vehicle_data['link_id', 'brand', 'model', 'model_name',
                                'vehicle_region','generation', 'body', 'status', 'spec_name',
                                 'prod_dates', 'price', 'engine_code',
                                 'body_code', 'vehicle', 'type', 'spec']
        except Timeout:
            print('Ошибка таймаута')
        except  ConnectionError:
            print('Ошибка соединения')
        except:
            print('Не опознанная ошибка')
            time.sleep(5)
    return base_vehicle_data

def get_sub_vehicle_data(spec_row):
    link = spec_row[13]

    if link == "no specs" or link == "No specs" or link == "No_spec" or link == "no_spec":
        df_sub = pd.DataFrame(columns = ['link_id'])
        df_sub = df_sub.append(
            {'link_id':link, 'spec_type': "no specs",
             'spec': 'no specs'}, ignore_index=True)
    else:
        try:
            df_sub = pd.read_html(link)[0]
            df_sub['link_id'] = link
            df_sub = df_sub.rename(columns={0: 'spec_type', 1: 'spec'})
            df_sub = df_sub[['link_id', 'spec_type', 'spec']]
        except:
            d = {'link_id': link,'spec_type': "table error", 'spec': "table error"}
            df_sub = pd.DataFrame(data=d, index=[0])
    return df_sub