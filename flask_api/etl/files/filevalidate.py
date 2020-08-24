import os
import pandas as pd

class Filevalidate():
    def file_exists_check (self, filename):
        return os.path.isfile (filename)

    def non_empty_file_check (self,filename):
        return os.path.getsize (filename) > 0

    def file_extension_check (self, filename):
        file_extention=os.path.splitext (filename) [1] [1:]
        return file_extention in ("xlsx","csv","txt")

