
import func
import parser
import htmls
import dir

def main():
    path = func.create_folder(dir.core_dir)
    brandpath = func.find_brand(path)
    modelpath = func.find_model(path, brandpath)
    genpath = func.find_gen(path, modelpath)
    specpath = func.find_spec(path, genpath)
    base_path = func.find_base(path, specpath)

if __name__ == '__main__':
    main()