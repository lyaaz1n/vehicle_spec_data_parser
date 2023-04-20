import func_noclass
import dir

def main():
    path = func_noclass.create_folder(dir.core_dir)
    brandpath = func_noclass.find_brand(path)
    modelpath = func_noclass.find_model(path, brandpath)
    genpath = func_noclass.find_gen(path, modelpath)
    specpath = func_noclass.find_spec(path, genpath)
    base_path = func_noclass.find_base(path, specpath)
    sub_path = func_noclass.find_sub(path, specpath)

if __name__ == '__main__':
    main()