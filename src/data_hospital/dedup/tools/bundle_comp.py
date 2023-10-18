from tqdm import tqdm 
import json

def gen_clip_id(carid, ts):
    """
    [[0, 9], [10, 19], [20, 29], [30, 39], [40, 49], [50, 59]]
    """
    ts = str(ts)[:10]
    start_ts = "{}0".format(ts[:-1])
    end_ts = "{}9".format(ts[:-1])
    return "{}_{}_{}".format(carid, start_ts, end_ts)
    
    
if __name__ == "__main__":
    image_root = "/mnt/ve_share/songyuhao/generation/data/icu30/bundle_and_clip_info/out_sampled_10f.txt"
    
    with open(image_root, "r") as input_file:
        json_paths = [_.strip().split(",")[-1] if _.startswith("HP") else _.strip() for _ in input_file.readlines()]
        json_paths = [_ if _.startswith("/") else "/" + _ for _ in json_paths]
    # print(json_paths[:10])
    print("Total Json Amout: %d" % len(json_paths))
    for json_path in tqdm(json_paths):
        with open(json_path, "r") as input_json:
            json_obj = json.load(input_json)
            timestamp = json_obj["trigger_time"]
            carid = json_obj["carid"]
            print(gen_clip_id(carid=carid, ts=timestamp))
