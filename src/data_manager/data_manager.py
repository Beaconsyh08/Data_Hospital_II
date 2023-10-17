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

import pandas as pd
from tqdm import tqdm


class DataManager:
    """
    Summary
    -------
        An instance of this class could manage the data in a form of pandas.DataFrame from jsons
    """
    
    def __init__(self, df: pd.DataFrame = None, df_path: str = None) -> None:
        """
        Summary
        -------
            The Constructor for the class, but how to build it
        
        Parameters
        ----------
            df (optional): pd.DataFrame, default = "None"
                the dataframe
            df_path (optional): str, default = "None"
                the dataframe pickle pat
            cfg (optional): dict, default = "None"
                the config dict
            
        """

        self.df = df if df_path is None else load_from_pickle(df_path)


    def getter(self) -> pd.DataFrame:
        """
        Summary
        -------
            Return the dataframe of DataManager instance, equal to instance.df
        
        Returns
        -------
            pd.DataFrame
            
        """
        
        return self.df
    

    def get_cols(self, cols: List[str]) -> pd.DataFrame:
        """
        Summary
        -------
            return wantted category info in dataframe

        Parameters
        ----------
            cols: List[str]
                list of string of wantted category

        Returns
        -------
            pd.DataFrame
        
        """
        
        return self.df.loc[:, cols]

    
    def get_col_values(self, col_name: str) -> list:
        """
        Summary
        -------
            Return the wanted one column in list fo value
        
        Parameters
        ----------
            col_name: str
                the name of the specific column

        Returns
        -------
            list: the list of wantted value
            
        """
        
        return self.df[col_name].to_list()
    

    def get_rows(self, rows: List[str]) -> pd.DataFrame:
        """
        Summary
        -------
            Return the wantted rows in dataframe

        Parameters
        ----------
            rows: List[str]
                list of string of wantted rows

        Returns
        -------
            pd.DataFrame
        
        """
        
        return self.df.loc[rows, :]


    def sampler(self, n: int) -> None:
        """
        Summary
        -------
            Sampling and resize the dataframe

        Parameters
        ----------
            n: int
                sampel to n rows
                
        """
        
        self.df = self.df.sample(n = n)


    @abstractmethod
    def json_extractor(self, json_info: json, json_path: str, count: int) -> List[dict]:
        """
        Summary
        -------
            Abstract method for the subclass using
            Extracto the json and return list of objects(instances)

        Parameters
        ----------
            json_info: json
                the json object
            json_path: str
                the json path in string

        Returns
        -------
            List[dict]: a list of instances, info in dict
        
        """

        pass
    
    
    def load_from_json(self, data_type: str = None, json_type: str = None, load_path: str = None) -> None:
        """
        Summary
        -------
            Loading from json
        
        Parameters
        ----------
            data_type (optional): str, default = "None"
                type of the data, chosen from ["qa", "train", "ret", "inf"]
            json_type (optional): str, default = "None"
                type of the json data, chosen from ["txt", "folder"]
            json_path (optional): str, default = "None"
                the path of json 
        """

        # set parameters from config dict
        data_type = self.cfg.DATA_TYPE if data_type is None else data_type
        json_type = self.cfg.JSON_TYPE if json_type is None else json_type
        load_path = self.cfg.JSON_PATH if load_path is None else load_path

        def __json_path_getter(load_path: str) -> list:
            """
            Summary
            -------
                get the json paths from txt or folder

            Parameters
            ----------
                json_type: str
                    choose from ["txt", "folder"]
                load_path: str
                    path of the jsons

            Returns
            -------
                list: list of json pahts
                
            """

            if json_type.lower() == "txt":
                json_paths = list(open(load_path, "r"))

            elif json_type.lower() == "folder":
                json_paths = [load_path + "/" + _ for _ in os.listdir(load_path)]

            else:
                self.logger.critical("Make sure the json_type is chosen from ['txt', 'folder'], you could suggest more json_type to be added")               
            
            return json_paths


        def __json_reader(json_paths: list) -> None:
            """
            Summary
            -------
                read each json object, pass the object to the json extractor
                
            Parameters
            ----------
                json_paths: list
                    list of json paths
                    
            """

            combined_lst = []
            def worker(_):
                json_path = _.strip()
                with open(json_path) as json_obj:
                    try:
                        json_info = json.load(json_obj)
                        json_lst = self.json_extractor(json_info, json_path)
                        combined_lst.extend(json_lst)
                    except json.JSONDecodeError:
                        self.logger.error("JSONDECODEERROR: %s" % json_path)
                    
            with ThreadPool(processes = 40) as pool:
                list(tqdm(pool.imap(worker, json_paths), total=len(json_paths), desc='DataFrame Loading'))
                pool.terminate()
                    
            self.df = pd.DataFrame(combined_lst)
            try:
                self.df = self.df.set_index("index_list")
            except KeyError:
                self.logger.error("Perhaps the DataFrame Contains No Data")
                sys.exit(-1)
            
        self.logger.critical("DataFrame Loading Started: %s" % load_path)
        json_paths = __json_path_getter(load_path)

        __json_reader(json_paths)
        print(self.df)
            
        self.logger.info("DataFrame Loading Finished")
        self.logger.info("DataFrame shape is: %s" % str(self.df.shape))
    

    def save_to_pickle(self, save_path: str, info_dict: dict = None) -> None:
        """
        Summary
        -------
            Save the dataframe and the info of clustering into pickle
            
        Parameters
        ----------
            save_path: str
                the path to save the pickle
            info_dict (optional): dict, default = "None" 
                the info of the clustring
                
        """
        
        if save_path:
            os.makedirs("/".join(save_path.split("/")[:-1]), exist_ok=True)
        try:
            info_dict = load_from_pickle(save_path, "info") if info_dict is None else info_dict
        except:
            info_dict = None

        pickle_obj = {"info": info_dict, "df": self.df}
        with open(save_path, "wb") as pickle_file: 
            pickle.dump(pickle_obj, pickle_file)
        self.logger.debug("DataFrame Saved: %s" % save_path)    
        

    def merge_dataframe_cols(self, new_columns: pd.DataFrame, merge_style: str) -> None:
        """
        Summary
        -------
            merge new columns to the original self DataFrame
            
        Parameters
        ----------
            new_columns: pd.DataFrame
                the new columns to be added in the format of pd.DataFrame based on different keys.
            merge_style: str
                Merge style include {'left', 'right', 'outer', 'inner', 'cross'}
                left: use only keys from left frame, similar to a SQL left outer join; preserve key order.
                right: use only keys from right frame, similar to a SQL right outer join; preserve key order.
                outer: use union of keys from both frames, similar to a SQL full outer join; sort keys lexicographically.
                inner: use intersection of keys from both frames, similar to a SQL inner join; preserve the order of the left keys.
                cross: creates the cartesian product from both frames, preserves the order of the left keys.
                
        """
        self.df = self.df.merge(new_columns, how=merge_style, left_index=True, right_index=True)
    

    def add_columns(self, cols: list, col_labels: list) -> None:
        for i, each in enumerate(cols):
            self.df[col_labels[i]] = each          
            

    def delete_columns(self, delete_key_lst: list) -> None:
        self.df = self.df.drop(delete_key_lst, axis=1)


    def add_rows(self, new_rows: pd.DataFrame) -> None:
        self.df = self.df.append(new_rows)


    def delete_rows(self, delete_index_lst: list) -> None:
        self.df = self.df.drop(delete_index_lst, axis=0)


    def data_updater(self, index, column, new_value) -> None:
        # update certain column
        self.df.at[index, column] = new_value


    def convert_to_json(self, ) -> json:
        return self.df.to_json(orient="index")
        

def merge_dataframe_rows(df_1: pd.DataFrame, df_2: pd.DataFrame) -> DataManager:
    """
    Summary
    -------
        Merge two dataframes by rows, add None to diff columns
        
    Parameters
    ----------
        one_df: pd.DataFrame
        another_df: pd.DataFrame
    
    Returns
    -------
        DataManager: the DataManager instance of combined dataframe
    
    """
    
    temp_ade = DataManager(pd.concat([df_1, df_2], axis=0))
    print(temp_ade)
    return temp_ade


def load_from_pickle(load_path: str, dforinfo: str = "df") -> pd.DataFrame or dict:
    """
    Summary
    -------
        Load info dict of DataFrame instance from pickle
        
    Parameters
    ----------
        load_path: str
            the path to load pickle, either DataFrame of dict
        dforinfo (optional): str, default = "df"
            either "df" or "info" for now
        
    Returns
    -------
        pd.DataFrame of dict: depends on dforinfo
        
    """
    
    with open(load_path, "rb") as pickle_file:
        pickle_obj = pickle.load(pickle_file)
        if type(pickle_obj) == dict:
            return pickle_obj[dforinfo]
        else:
            return pickle_obj
    
