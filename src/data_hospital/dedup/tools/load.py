path = "/mnt/ve_share/songyuhao/generation/data/result/dedup/clip_p5_total/paths.pkl"

import pickle
with open(path, "rb") as pickle_file:
    pickle_obj = pickle.load(pickle_file)

print(pickle_obj)
left = [kv for kv in pickle_obj.items() if len(kv[-1]) == 0]
print("Left Amount: %d" % len(left))