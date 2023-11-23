import sys
sys.path.append("./src")
import os
import uuid
import pickle
from imagededup.utils.general_utils import get_files_to_remove
from data_hospital.dedup.tools.data_dedup import save_to_pickle, load_from_pickle
from data_manager.data_manager_nerf import DataManagerNerf
from data_manager.data_manager_lane import DataManagerLane
import json
from tqdm import tqdm


def create_dm(data_type, json_type, load_path, view_points):
    if data_type == "nerf":
        dm = DataManagerNerf(data_type=data_type, json_type=json_type, load_path=load_path, view_points=view_points)
    elif data_type == "lane":
        dm = DataManagerLane(data_type=data_type, json_type=json_type, load_path=load_path, view_points=view_points)
    dm.load_from_json()
    return dm


def upload_to_tos(dm, data_type):
    uuid_path = "%s.xlsx" % uuid.uuid4()
    output_path = "/root/tmp/%s" % uuid_path
    tos_path = "tos://haomo-lsu-search/LIV/syh/online_services/data_hospital_2/%s/%s" % (data_type, uuid_path)
    dm.df.to_excel(output_path)
    os.system("/root/tosutil cp %s %s" % (output_path, tos_path))
    os.remove(output_path)
    return tos_path


def process_df(save_root: str):
    
    duplicates = load_from_pickle("%s/duplicates.pkl" % save_root)
    
    files_to_remove = get_files_to_remove(duplicates)
    print("Duplicated Images: %d" % len(files_to_remove))
    save_to_pickle(save_path="%s/duplicated_images.pkl" % save_root, pickle_obj=files_to_remove)


if __name__ == "__main__":
    data_type = "lane"
    json_type = "txt_img"
    load_path = "/mnt/ve_share/songyuhao/lanes/data/train/v0.1_img.json"
    view_points = ["front_middle_camera_record"]
    
    output_root = "/mnt/ve_share/songyuhao/generation/data/result/dedup/lanes"
    # os.makedirs(output_root, exist_ok=True)
    
    # process_df(save_root=output_root)
    with open (load_path, "r") as input_file:
        json_info = json.load(input_file)
        
    combined_dict = {k: v for d in json_info for k, v in d.items()}
    all_imgs = set(combined_dict.keys())
        
    files_to_remove = set(load_from_pickle("/mnt/ve_share/songyuhao/generation/data/result/dedup/lanes/duplicated_images.pkl"))
    files_to_remove = {"/" + _.replace(":/", "://") for _ in files_to_remove}
    
    no_dup = all_imgs - files_to_remove
    print(len(no_dup))
    
    output_path = "%s/no_dup.txt" % output_root
    with open(output_path, "w") as output_file: 
        for img_url in tqdm(no_dup):
            json_p = combined_dict[img_url]
            output_file.writelines(json_p + "\n")
        

    
    
    # tos_path = upload_to_tos(dm=dm, data_type=data_type)
    # print("Your Result Has Been Saved in: %s" % tos_path)
    
