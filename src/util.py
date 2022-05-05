import pandas as pd
import os 

def get_path():
    print(os.getcwd().replace('\\','/'))