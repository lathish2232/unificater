import pandas as pd
import os
from .filevalidate import Filevalidate

from utility.logger import log

class Fileprocess (Filevalidate):

    def __init__ (self,file_details):
        self.file_details = file_details
    
    def get_file_type (self, filename) :
        return os.path.splitext (filename) [1] [1:]
    
    def validate(self):
        err_msg = None
        if not self.file_exists_check(self.file_details['file']):
            err_msg = "File is not exists in this path,please give correct file path details"
        elif not self.non_empty_file_check(self.file_details['file']):
            err_msg="File is empty,please give correct non empty file"
        elif not self.file_extension_check(self.file_details['file']):
            err_msg="Supported file extensions excel,csv,txt only"
        if not err_msg:
            err_msg="successful"

        return err_msg

    def excel_file_process (self):
        if self.file_details["sheet_ind"]=="no":
            output=pd.ExcelFile(self.file_details['file']).sheet_names
        elif self.file_details["sheet_ind"]=="yes":
            output=pd.read_excel(self.file_details['file'],sheet_name=self.file_details['sheet_name']).to_dict (orient="records")
        return output

    def non_excel_file_process (self):
        if self.file_details["delimiter_ind"]=="no":
            with open(self.file_details['file']) as f:
                output=f.readlines ()

        else:
            if self.get_file_type(self.file_details['file']) in ("csv","txt") and self.file_details['fwf_type']=="no":
                output=pd.read_csv(self.file_details['file'],sep=self.file_details['delimiter']).to_dict(orient="records")
            elif self.get_file_type(self.file_details['file']) == "txt" and self.file_details['fwf_type']=="yes":
                output= pd.read_fwf (self.file_details['file'],colspecs=self.file_details['colspecs']).to_dict (orient="rescords")
            else:
                output="invalid file,please check the file content"
        return output


