import os
import pandas as pd


class SaveFileDebug:
    def __init__(self, path, filename):

        self.path = path
        self.filename = filename

    def save(self, df):

        try:

            if not os.path.exists(self.path):
                os.makedirs(self.path)
                print(f"Directorio creado: {self.path}")
            full_path = os.path.join(self.path, self.filename)
            df.to_csv(full_path, index=False)
            print(f"Data saved to {full_path}")
        except Exception as e:
            print(f"Error saving file to {self.path}/{self.filename}: {e}")
            raise
