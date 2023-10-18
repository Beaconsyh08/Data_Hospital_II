import os
import sys
sys.path.append("./src")
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import uuid
import copy


from data_manager.data_manager_nerf import DataManagerNerf
from data_hospital.dedup.tools.data_dedup import process_df
import os
import uuid

app = Flask(__name__)
CORS(app)


def create_dm(data_type, json_type, load_path, view_points):
    dm = DataManagerNerf(data_type=data_type, json_type=json_type, load_path=load_path, view_points=view_points)
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


@app.route('/backend/algorithm/data_hospital_2_run', methods=['POST', 'GET'])
def data_hospital_2_run():
    """
    input data example:
    {
        "header": {
            "timestamp": 1688636992401453
        },
        "message": {
            "images_txt_path": "xxx.txt",
            "data_type": "nerf",
            "json_type": "txt_img"
        }
    }
    """
    input_data = request.get_json()
    message = input_data['message']
    images_txt_path = message['images_txt_path']
    data_type = message['data_type']
    json_type = message['json_type']
    view_points = [""]

    output_data = copy.deepcopy(input_data)
    
    if data_type == "nerf" and "json_type" == "txt_img":
        dm = create_dm(data_type=data_type, json_type=json_type, load_path=images_txt_path, view_points=view_points)
        
        output_root = "/mnt/ve_share/songyuhao/generation/data/result/dedup/nerf"
        os.makedirs(output_root, exist_ok=True)
        
        dm = process_df(dm=dm, save_root=output_root, method="PHash")
        
        output_excel_tos = upload_to_tos(dm=dm, data_type=data_type)
        
        output_data['output'] = {'output_excel_tos': output_excel_tos}
        
        return jsonify(output_data)
        
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
    