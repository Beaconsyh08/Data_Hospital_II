import sys
sys.path.append("./src")
from data_manager.data_manager_nerf import DataManagerNerf
from data_manager.data_manager_lane import DataManagerLane
from data_hospital.dedup.tools.data_dedup import process_df
import os
import uuid


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


if __name__ == "__main__":
    data_type = "lane"
    json_type = "txt_img"
    load_path = "/mnt/ve_share/songyuhao/lanes/data/train/v0.1_img.txt"
    view_points = ["front_middle_camera_record"]
    
    dm = create_dm(data_type=data_type, json_type=json_type, load_path=load_path, view_points=view_points)
    
    output_root = "/mnt/ve_share/songyuhao/generation/data/result/dedup/lanes"
    os.makedirs(output_root, exist_ok=True)
    
    dm = process_df(dm=dm, save_root=output_root, method="PHash")
    
    tos_path = upload_to_tos(dm=dm, data_type=data_type)
    print("Your Result Has Been Saved in: %s" % tos_path)
    
