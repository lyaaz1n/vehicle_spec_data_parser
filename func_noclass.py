import parser_noclass
import pandas as pd
from datetime import datetime
import multiprocessing as mp
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

def read_csv(path):
    reader = pd.read_csv(path, sep=";")
    return reader

def create_folder(core_dir):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H.%M.%S")
    directory = 'data_exteract_' + dt_string
    path = os.path.join(core_dir, directory)
    os.mkdir(path)
    print('Directory %s has been crated' % directory)
    return path

def find_brand(path):
    print('Brands has been started')

    link = 'https://www.website.com/'
    filename = 'brand.csv'
    directory = 'brand'

    dirpath = os.path.join(path, directory)
    brandpath = os.path.join(dirpath, filename)
    os.mkdir(dirpath)

    brands = parser_noclass.get_brand_data(link)
    brands.to_csv(brandpath, encoding='utf-8-sig', sep=";", index=False)
    print('Brands has been finished')
    return brandpath

def find_model(path, brandpath):
    print("Models has been started")
    models = pd.DataFrame()
    filename = 'model.csv'
    directory = 'model'
    dirpath = os.path.join(path, directory)
    os.mkdir(dirpath)
    modelpath = os.path.join(dirpath, filename)

    brands = read_csv(brandpath)
    w = []
    for i in brands.itertuples():
        w.append(list(i))

    pool = mp.Pool(processes=(mp.cpu_count() - 1))
    results = pool.map(parser_noclass.get_model_data, w)
    pool.close()
    pool.join()
    results_df = pd.concat(results)

    results_df.to_csv(modelpath, encoding='utf-8-sig', sep=";", index=False)
    print('Models has been finished')
    return modelpath

def find_gen(path, modelpath):
    print("Generations has been started")
    generations = pd.DataFrame()
    directory = 'generation'
    dirpath = os.path.join(path, directory)
    os.mkdir(dirpath)

    namechunk = 'generation_'
    counter = 1

    chunksize = 100
    chunks = len(pd.read_csv(modelpath, sep=";").index) / 30

    reader = pd.read_csv(modelpath, sep=";", chunksize=chunksize)
    for chunk in reader:
        models = pd.DataFrame(chunk)
        w = []
        for i in models.itertuples():
            w.append(list(i))
        pool = mp.Pool(processes=(mp.cpu_count() - 1))
        results = pool.map(parser_noclass.get_generations_data, w)
        pool.close()
        pool.join()
        generations = pd.concat(results)
        #generations = parser.Gen.get_data(a)

        filename = namechunk + str(counter) + '.csv'
        counter = counter + 1
        genpath = os.path.join(dirpath, filename)
        generations.to_csv(genpath, encoding='utf-8-sig', sep=";", index=False)
        print(str(counter) + ' of ' + str(chunks) + ' chunks complete!')

    print('Generations has been finished')
    return dirpath

def find_spec(path, genpath):
    print("Specs has been started")
    specs = pd.DataFrame()
    directory = 'specs'
    dirpath = os.path.join(path, directory)
    os.mkdir(dirpath)
    namechunk = 'spec_'
    counter = 1

    chunksize = 60
    filecount = len(os.listdir(genpath))
    for filename in os.listdir(genpath):
        file = os.path.join(genpath, filename)
        reader = pd.read_csv(file, sep=";", chunksize=chunksize)
        for chunk in reader:
            gen = pd.DataFrame(chunk)

            w = []
            for i in gen.itertuples():
                w.append(list(i))

            pool = mp.Pool(processes=(mp.cpu_count() - 1))
            results = pool.map(parser_noclass.get_spec_data, w)
            pool.close()
            pool.join()
            specs = pd.concat(results)

            filename = namechunk + str(counter) + '.csv'
            specpath = os.path.join(dirpath, filename)
            specs.to_csv(specpath, encoding='utf-8-sig', sep=";", index=False)

        counter = counter + 1
        print(str(counter) + ' of ' + str(filecount) + ' files complete!')

    print('Specs has been finished')
    return dirpath

def find_base(path, specpath):
    base = pd.DataFrame()
    directory = 'base_data'
    dirpath = os.path.join(path, directory)
    os.mkdir(dirpath)
    namechunk = 'base_data_'
    file_counter = 1
    chunksize = 80
    filecount = len(os.listdir(specpath))
    for filename in os.listdir(specpath):
        file = os.path.join(specpath, filename)
        reader = pd.read_csv(file, sep=";", chunksize=chunksize)

        # row_num = sum(1 for row in open(file))
        # chunks_no = round(row_num / chunksize, 0)
        chunk_counter = 1
        for chunk in reader:
            spec = pd.DataFrame(chunk)

            w = []
            for i in spec.itertuples():
                w.append(list(i))

            pool = mp.Pool(processes=(mp.cpu_count() - 1))
            results = pool.map(parser_noclass.get_base_vehicle_data, w)
            pool.close()
            pool.join()
            base = pd.concat(results)

            filename = namechunk + 'file_' + str(file_counter) + '_chunk_' + str(chunk_counter) + '.csv'

            chunk_counter = chunk_counter + 1
            basepath = os.path.join(dirpath, filename)
            base.to_csv(basepath, encoding='utf-8-sig', sep=";", index=False)

        file_counter = file_counter + 1
        print(str(file_counter) + ' of ' + str(filecount) + ' files complete!')
    print('Base data has been finished')
    return dirpath

def find_sub(path, specpath):
    print('Sub data has been started')
    sub = pd.DataFrame()
    directory = 'sub_data'
    dirpath = os.path.join(path, directory)
    os.mkdir(dirpath)
    namechunk = 'sub_data_'
    file_counter = 1
    chunksize = 80
    filecount = len(os.listdir(specpath))
    for filename in os.listdir(specpath):
        file = os.path.join(specpath, filename)
        reader = pd.read_csv(file, sep=";", chunksize=chunksize)

        # row_num = sum(1 for row in open(file))
        # chunks_no = round(row_num / chunksize, 0)
        chunk_counter = 1
        for chunk in reader:
            spec = pd.DataFrame(chunk)

            w = []
            for i in spec.itertuples():
                w.append(list(i))

            pool = mp.Pool(processes=(mp.cpu_count() - 1))
            results = pool.map(parser_noclass.get_sub_vehicle_data, w)
            pool.close()
            pool.join()
            base = pd.concat(results)

            filename = namechunk + 'file_' + str(file_counter) + '_chunk_' + str(chunk_counter) + '.csv'

            chunk_counter = chunk_counter + 1
            basepath = os.path.join(dirpath, filename)
            base.to_csv(basepath, encoding='utf-8-sig', sep=";", index=False)

        file_counter = file_counter + 1
        print(str(file_counter) + ' of ' + str(filecount) + ' files complete!')
    print('Sub data has been finished')
    return dirpath