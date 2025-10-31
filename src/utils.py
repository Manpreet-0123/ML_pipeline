import os
import pickle

def save_object(model,model_path):
    file_path = os.path.dirname(model_path)

    os.makedir(file_path,exist_ok=True)

    with open(model_path,"wb") as f:
        pickle.dump(model,f)