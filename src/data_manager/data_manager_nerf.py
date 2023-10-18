#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@File    :   analysis_data_engine.py
@Time    :   2021/11/11 16:48:51
@Email   :   songyuhao@haomo.ai
@Author  :   YUHAO SONG 

Copyright (c) HAOMO.AI, Inc. and its affiliates. All Rights Reserved
"""


import json
import os
import pickle
import sys
from abc import abstractmethod
from multiprocessing.pool import ThreadPool
from typing import List
from pathlib import Path
from data_manager.data_manager_base import DataManagerBase
from pathlib import Path

import pandas as pd
from tqdm import tqdm


class DataManagerNerf(DataManagerBase):
    """
    Summary
    -------
        An instance of this class could manage the data in a form of pandas.DataFrame from jsons
    """
    def __init__(self, data_type: str = None, json_type: str = None, load_path : str = None, view_points : list = None) -> None:
        """
        Summary
        -------
            The Constructor for the class, but how to build it
        
        Parameters
        ----------

            
        """
        super().__init__(data_type, json_type, load_path, view_points)
        

    def json_extractor(self, json_path: str) -> dict:
        """
        Summary
        -------
            Abstract method for the subclass using
            Extracto the json and return list of objects(instances)

        Parameters
        ----------
            json_path: str
                the json path in string

        Returns
        -------
            dict: info in dict
        
        """
        
        if self.json_type == "txt":
            with open(json_path) as input_json:
                json_obj = json.load(input_json)
            
            cam_objs = json_obj["camera"]
            clip_id = json_obj["label_clip_id"]
            res_dict = dict()
            
            if cam_objs:
                for cam_obj in cam_objs:
                    view_point = cam_obj["name"]
                    if view_point in self.view_points:
                        res_dict['view_point'] = view_point
                        res_dict["clip_id"] = clip_id
                        res_dict["json_path"] = json_path
                        res_dict["img_path"] = Path("/" + cam_obj["oss_path"])

                return res_dict
            else:
                return None
        elif self.json_type == "txt_img":
            json_path = json_path if json_path[0] == "/" else "/" + json_path
            res_dict = dict()
            res_dict["img_path"] = Path(json_path)
            
            return res_dict