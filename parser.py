import requests as rq
from bs4 import BeautifulSoup
import lxml
import pandas as pd
import time
import os
from datetime import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
import urllib3, socket
from urllib3.connection import HTTPConnection
HTTPConnection.default_socket_options = (HTTPConnection.default_socket_options + [
            (socket.SOL_SOCKET, socket.SO_SNDBUF, 10000000), #1MB in byte
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 10000000)
        ])
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import htmls

class Parser:
    def __init__(self, link=None, brand=None):
        self.link = link
        self.brand = brand


class Brand(Parser):
    def get_data(self):
        brands = pd.DataFrame()
        link = self.link
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


class Model(Parser):
    def get_data(self):
        models = pd.DataFrame(columns=['brand', 'model', 'link'])
        g = self.link
        br = self.brand  # Brand name
        print(g)
        print(br)
        try:
            responce = rq.get(g).text
            soup = BeautifulSoup(responce, 'lxml')
        except:
            time.sleep(1)
            print(str(g) + ' no responce')

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
                        models = models.append({'brand': br, 'model': modelname, 'link': lnk}, ignore_index=True)

        return models


class Gen(Parser):
    def get_data(self):
        generations = pd.DataFrame()
        for i in self.table.itertuples():
            link = i[3]
            brand = i[1]
            mdl = i[2]
            try:
                responce = rq.get(link, timeout=5).text
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
            except:
                print("No coonection to this generation: " + link)
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

                            if i.find(htmls.base_generation_model_name[0], class_=htmls.base_generation_model_name[1]) is None:
                                model_name = 'none'
                                print('base_generation_model_name tag is empty')
                            else:
                                model_name = i.find(htmls.base_generation_model_name[0], class_=htmls.base_generation_model_name[1]).text
                                print(model_name)

                            sub_data = i.find_all(htmls.base_generation_sub_data[0], class_=htmls.base_generation_sub_data[1])
                            gen_name = sub_data[0].text
                            body_type = sub_data[1].text
                            try:
                                vehicle_status = i.find(htmls.base_generation_vehicle_status[0], class_=htmls.base_generation_vehicle_status[1]).text
                            except:
                                vehicle_status = "inactive"


                            lnk = link + l
                            generations = generations.append({'brand':brand, 'model':mdl,
                            'vehicle_region':region, 'model_name':model_name, 'generation':gen_name,
                            'body':body_type, 'status':vehicle_status, 'link':lnk}, ignore_index = True)


        generations['model_name'] = generations['model_name'].astype(str).str.strip()
        generations['model_name'] = generations['model_name'].str.replace(" ", "_")
        generations['model_name'] = generations['model_name'].str.replace(r'\n', '_', regex=True)
        generations['model_name'] = generations['model_name'].str.replace(r'\r', '_', regex=True)

        return generations


class Spec(Parser):
    def get_data(self):
        spec = pd.DataFrame()
        count = len(self.table.index)
        counter = 0
        for i in self.table.itertuples():
            g = i[8]
            brnd = i[1]
            mdl = i[2]
            region = i[3]
            mdl_nm = i[4]
            gn = i[5]
            bd = i[6]
            sts = i[7]
            try:
                responce = rq.get(g, timeout=2).text
                # print("Spec response is fine: " + g)
            except:
                time.sleep(2)
                print("Spec response sucks with this one: " + g)
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
                                lnk = 'https://www.drom.ru' + lnk
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


class BaseData(Parser):
    def get_data(self):
        from requests.exceptions import Timeout, ConnectionError

        headers = {
            'User-Agent': htmls.header_mozila}
        global responce
        base_vehicle_data = pd.DataFrame()
        for i in self.table.itertuples():
            link = i[13]
            brand = i[1]
            model = i[2]
            model_name = i[3]
            region = i[4]
            generation = i[5]
            body = i[6]
            status = i[7]
            spec_name = i[8]
            prod_dates = i[9]
            price = i[10]
            engine_code = i[11]
            body_code = i[12]
            # print('link is: ' + str(link))
            # print('brand is: ' + str(brand))
            # print('model is: ' + str(model))
            # print('model name is: ' + str(model_name))
            # print('region is: ' + str(region))
            # print('gen is: ' + str(link))

            if link == "no specs" or link == "No specs" or link == "No_spec" or link == "no_spec" or link == "none":
                base_vehicle_data = base_vehicle_data.append(
                    {'brand': brand, 'model': model, 'model_name': model_name, 'vehicle_region': region,
                     generation: 'generation', 'body': body, 'status': status, 'spec_name': spec_name,
                     'prod_dates': prod_dates, 'price': price, 'engine_code': engine_code, 'body_code': body_code,
                     'vehicle': "no specs", 'Type': "no specs", 'Spec': "no specs"}, ignore_index=True)
                # print("No link ")
                # print(brand + model)
                # counter = counter + 1
                # print("complete %: " + str((counter / count)*100))
            else:
                # print('here we go with ')
                # print(brand + model)
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
                                        {'brand': brand, 'model': model, 'model_name': model_name, 'vehicle_region': region,
                                         'generation': generation, 'body': body, 'status': status, 'spec_name': spec_name,
                                         'prod_dates': prod_dates, 'price': price, 'engine_code': engine_code,
                                         'body_code': body_code, 'vehicle': header, 'Type': cols, 'Spec': spec},
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


                except Timeout:
                    print('Ошибка таймаута')
                except  ConnectionError:
                    print('Ошибка соединения')
                except:
                    print('Не опознанная ошибка')
                    time.sleep(5)

        return base_vehicle_data
