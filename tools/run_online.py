import copy
import os
import sys
import json
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS
import PIL
import torch
import base64
import io
from diffusers import StableDiffusionControlNetInstructPix2PixPipeline, UniPCMultistepScheduler, UNet2DConditionModel, ControlNetModel
import os
import uuid
from img_blur_processor import IMGBlurProcessor
from io import BytesIO
import cv2
import numpy as np
from PIL import Image


app = Flask(__name__)
CORS(app)

@app.route('/backend/algorithm/data_hospital_2_run', methods=['POST', 'GET'])
def data_hospital_2_run():
    """
    input data example:
    {
        "header": {
            "timestamp": 1688636992401453
        },
        "message": {
            "images_txt_path": xxx.txt
        }
    }
    """
    input_data = request.get_json()
    message = input_data['message']
    images_txt_path = message['images_txt_path']
    
    

