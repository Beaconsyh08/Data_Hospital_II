import sys
sys.path.append("./src")
from data_hospital.dedup.imagededup.methods import PHash, AHash, DHash, WHash, CNN
from tqdm import tqdm
import os
import json
from imagededup.utils import CustomModel
from imagededup.utils.models import ViT, MobilenetV3, EfficientNet
from imagededup.utils import plot_duplicates
from pathlib import Path
import time
from multiprocessing.pool import ThreadPool
import pickle
import itertools
from imagededup.utils.general_utils import get_files_to_remove


def initialization(method: str):
    if method == "PHash":
        encoder = PHash()
    elif method == "AHash":
        encoder = AHash()
    elif method == "DHash":
        encoder = DHash()
    elif method == "WHash":
        encoder = WHash()
    elif method == "ViT":
        custom_config = CustomModel(name=ViT.name, model=ViT(), transform=ViT.transform)
        encoder = CNN(model_config=custom_config)
    elif method == "MobilenetV3":
        custom_config = CustomModel(name=MobilenetV3.name, model=MobilenetV3(), transform=MobilenetV3.transform)
        encoder = CNN(model_config=custom_config)
    elif method == "EfficientNet":
        custom_config = CustomModel(name=EfficientNet.name, model=EfficientNet(), transform=EfficientNet.transform)
        encoder = CNN(model_config=custom_config)
    
    return encoder

def save_to_pickle(save_path, pickle_obj):
    with open(save_path, "wb") as pickle_file: 
        pickle.dump(pickle_obj, pickle_file)
    print("Pickle Saved: %s" % save_path)    
        
def load_from_pickle(pickle_path):
    with open(pickle_path, "rb") as pickle_file:
        pickle_obj = pickle.load(pickle_file)
    return pickle_obj

def find_duplicates(encoder, method: str, encodings: dict, max_distance_threshold=5, min_similarity_threshold=0.99) -> dict:
    if method.endswith("Hash"):
        duplicates = encoder.find_duplicates(encoding_map=encodings, max_distance_threshold=max_distance_threshold)
    else:   
        duplicates = encoder.find_duplicates(encoding_map=encodings, min_similarity_threshold=min_similarity_threshold)
        
    return duplicates

def find_duplicates_to_remove(encoder, method: str, encodings: dict, max_distance_threshold=5, min_similarity_threshold=0.99) -> list:
    if method.endswith("Hash"):
        duplicates_to_remove = encoder.find_duplicates_to_remove(encoding_map=encodings, max_distance_threshold=max_distance_threshold)
    else:
        duplicates_to_remove = encoder.find_duplicates_to_remove(encoding_map=encodings, min_similarity_threshold=min_similarity_threshold)
        
    return duplicates_to_remove

def find_duplicates_to_remove_dup():
    pass

def plot(duplicates: dict, output_root: str):
    # plot duplicates obtained for a given file using the duplicates dictionary
    for key in tqdm(duplicates.keys()):
        plot_duplicates(image_dir=image_root,
                        duplicate_map=duplicates,
                        filename=key,
                        outfile="%s/%s" % (output_root, key))
        

def load_jsons(json_paths:list, viewpoints:list):
    res_dict = {key: [] for key in viewpoints}
    def worker(_):
        cam_objs = None
        with open(_) as input_json:
            try:
                json_obj = json.load(input_json)
                cam_objs = json_obj["camera"]
                
            except json.decoder.JSONDecodeError:
                print(input_json)
            
            if cam_objs:
                for cam_obj in cam_objs:
                    if cam_obj["name"] in viewpoints:
                        print(cam_obj["oss_path"])
                        print(Path(cam_obj["oss_path"]))
                        res_dict[cam_obj["name"]] += [Path("/" + cam_obj["oss_path"])]
                    
    with ThreadPool(processes = 40) as pool:
        list(tqdm(pool.imap(worker, json_paths), total=len(json_paths), desc='JSON Loading'))
        pool.terminate()
        
    return res_dict


def combine_dict_keys(dict1, dict2):
    result_dict = {}
    # Loop through the keys in the dictionaries
    for key in dict1.keys():
        if key in dict2:
            # Concatenate the lists and store the result in the result_dict
            result_dict[key] = dict1[key] + dict2[key]
            
    return result_dict

def process(image_paths: list, save_root: str, method: str):
    start_time = time.time()
    
    # Generate encodings for all images in an image directory
    encoder = initialization(method=method)
    encodings = encoder.encode_images(image_paths=image_paths)
    save_to_pickle(save_path="%s/encodings.pkl" % save_root, pickle_obj=encodings)
    
    duplicates = find_duplicates(encoder=encoder, method=method, encodings=encodings)
    save_to_pickle(save_path="%s/duplicates.pkl" % save_root, pickle_obj=duplicates)
    
    # print("to_remove", len(duplicates_to_remove))
    # duplicates_to_remove = [Path("/" + _) for _ in duplicates_to_remove]
    # left = list(set(image_paths) - set(duplicates_to_remove))
    # print("left", len(left))
    
    # with open(res_dir, "a") as output_file:
    #     for enc in encodings.keys():
    #         output_file.writelines("%s#/%s#%s" % (carids[i], enc, list(encodings[enc])) + "\n")
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Elapsed time: {elapsed_time} seconds")
    
    
def process_from_mid(save_root: str, method: str):
    start_time = time.time()
    
    # Generate encodings for all images in an image directory
    encoder = initialization(method=method)
    
    encodings = load_from_pickle("%s/encodings.pkl" % save_root)
    encodings_new = dict()
    for i,(k,v) in tqdm(enumerate(encodings.items())):
        encodings_new[str(i)] = v
    encodings_new = dict(itertools.islice(encodings_new.items(), 400000))
    duplicates = find_duplicates_to_remove(encoder=encoder, method=method, encodings=encodings_new)
    save_to_pickle(save_path="%s/duplicates_to_rm.pkl" % save_root, pickle_obj=duplicates)
    
    # print("to_remove", len(duplicates_to_remove))
    # duplicates_to_remove = [Path("/" + _) for _ in duplicates_to_remove]
    # left = list(set(image_paths) - set(duplicates_to_remove))
    # print("left", len(left))
    
    # with open(res_dir, "a") as output_file:
    #     for enc in encodings.keys():
    #         output_file.writelines("%s#/%s#%s" % (carids[i], enc, list(encodings[enc])) + "\n")
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Elapsed time: {elapsed_time} seconds")
    
def process_df(dm, save_root: str, method: str):
    # Generate encodings for all images in an image directory
    encoder = initialization(method=method)
    encodings = encoder.encode_images(image_paths=list(dm.df["img_path"]))
    save_to_pickle(save_path="%s/encodings.pkl" % save_root, pickle_obj=encodings)
    
    duplicates = find_duplicates(encoder=encoder, method=method, encodings=encodings)
    save_to_pickle(save_path="%s/duplicates.pkl" % save_root, pickle_obj=duplicates)
    
    files_to_remove = get_files_to_remove(duplicates)
    print("Duplicated Images: %d" % len(files_to_remove))
    save_to_pickle(save_path="%s/duplicated_images.pkl" % save_root, pickle_obj=files_to_remove)
    
    duplicated_res = [item in files_to_remove for item in duplicates] 
    dm.add_columns([duplicated_res], ["duplicated"])
    print(dm.df)
        
    return dm
    
    
if __name__ == "__main__":
    # image_root = "/mnt/ve_share/songyuhao/generation/data/train/diffusions/5000/imgs"
    # image_root = "/mnt/ve_share/songyuhao/generation/data/exp"
    # image_root = "/mnt/ve_share/songyuhao/generation/data/icu30/bundle_and_clip_info/out_sampled_10f.txt"
    # image_root = "/mnt/ve_share/songyuhao/generation/data/icu30/bundle_and_clip_info/icu30_demo_0825.txt"
    image_root = "/mnt/ve_share/songyuhao/generation/data/icu30/bundle_and_clip_info/clip_1.txt"
    # image_root = "/mnt/ve_share/songyuhao/generation/data/icu30/bundle_and_clip_info/1m.txt"
    output_root = "/mnt/ve_share/songyuhao/generation/data/result/dedup/clip_p5_total"
    os.makedirs(output_root, exist_ok=True)

    # ["PHash", "AHash", "DHash", "WHash", "ViT", "MobilenetV3", "EfficientNet"]
    method = "PHash"
    viewpoints = ["front_middle_camera"]
    sampled_size = 100
    load_all = True
    res_txt = "/mnt/ve_share/songyuhao/generation/data/icu30/%s_%d.txt" % (method, sampled_size)
    
    # =========================================================================================
    dict_paths = {key: [] for key in viewpoints}
    carids = []
    if image_root.endswith(".hds") or image_root.endswith(".txt"):
        print("Loading")
        with open(image_root, "r") as input_file:
            inputs = input_file.readlines()
            
        json_paths = ["/" + _.strip().split(",")[-1] for _ in inputs]
        # json_paths = json_paths[:200000]
        print("Total Json Amout: %d" % len(json_paths))

        if load_all: 
            dict_paths = load_jsons(json_paths=json_paths, viewpoints=viewpoints)
            save_to_pickle(save_path="%s/paths.pkl" % output_root, pickle_obj=dict_paths)
            process(dict_paths[viewpoints[0]], save_root=output_root, method=method)
        else :
            for i in tqdm(range(0, len(json_paths), sampled_size)):
                dict_paths_t = load_jsons(json_paths=json_paths[i: i + sampled_size], viewpoints=viewpoints)
                process(dict_paths_t[viewpoints[0]], save_root=output_root, method=method)
                
    else:
        print("Check your input")
    # =========================================================================================
    
    # process_from_mid(save_root=output_root, method=method)
    