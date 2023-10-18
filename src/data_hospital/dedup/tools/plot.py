import pickle
from tqdm import tqdm
import os
from imagededup.utils import plot_duplicates
import random


para = "p0"
image_root = "/mnt/ve_share/songyuhao/generation/data/icu30/bundle_and_clip_info"
output_root = "/mnt/share_disk/songyuhao/data/data_dedup/%s" % para
os.makedirs(output_root, exist_ok=True)
pkl_root = "/mnt/ve_share/songyuhao/generation/data/result/dedup/clip_%s/duplicates.pkl" % para
ind_cor_path = "/mnt/ve_share/songyuhao/generation/data/result/dedup/clip_%s/paths.pkl" % para
find_specific = False
speicific_id = [57317]
sample_amouit = 500

with open(ind_cor_path, "rb") as pickle_file:
    pickle_obj = pickle.load(pickle_file)

paths_dict, tmp_paths = dict(), []
for i, each in enumerate(tqdm(pickle_obj["front_middle_camera"])):
    paths_dict[str(each)[1:]] = i
    if find_specific:
        if i == speicific_id:
            tmp_paths = str(each)[1:]

with open(pkl_root, "rb") as pickle_file:
    pickle_obj = pickle.load(pickle_file)

left = [kv for kv in pickle_obj.items() if len(kv[-1]) == 0]
print("Left Amount: %d" % len(left))

if find_specific:
    true_duplicates = {key: pickle_obj[key] for key in pickle_obj if key in tmp_paths}
else:
    true_duplicates = [kv for kv in pickle_obj.items() if len(kv[-1]) > 0]


if not find_specific:
    true_duplicates = dict(random.sample(true_duplicates, sample_amouit))

def plot(duplicates: dict, output_root: str):
    # plot duplicates obtained for a given file using the duplicates dictionary
    for key in tqdm(duplicates.keys()):
        plot_duplicates(image_dir="/",
                        duplicate_map=duplicates,
                        filename=key,
                        outfile="%s/%s" % (output_root, key.split(".")[0].split("/")[-1]),
                        paths_dict = paths_dict)

print(output_root)
plot(true_duplicates, output_root)
