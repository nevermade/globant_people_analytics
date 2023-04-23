import logging
import os
from hrpkg import data_operations
import config
import constants
from flask import Flask, request, render_template



logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=os.environ.get("LOGLEVEL", "INFO"))
app = Flask(__name__)

@app.route("/upload/<entity>",methods = ['GET','POST'])
def manage_(entity):
    if request.method == 'POST':
        uploaded_file= request.files['file']
        if uploaded_file.filename != '':
            uploaded_file.save('uploads/hired_employees_uploaded.csv')
            return "<h2>File was uploaded and proccessed succesfully<h2>"
    return f'''
        <!doctype html>
        <title>File Upload</title>
        <h1>Upload The file for: {entity}</h1>
        <form method=post enctype=multipart/form-data>
        <input type=file name=file>
        <input type=submit value=Upload>
        </form>
    '''

if __name__ == "__main__":
    
    app.run(host="0.0.0.0",port=3000, debug=True)
    # snow_op = data_operations.SnowflakeOperations(config.credentials)
    # snow_op.load_csv_to_snowflake_table(
    #     'uploads/hired_employees.csv', 'hired_employees', constants.HIRED_EMPLOYEES_DT)
    # snow_op.load_csv_to_snowflake_table(
    #     'uploads/hired_employees.csv', 'hired_employees', constants.HIRED_EMPLOYEES_DT)
    # snow_op.load_csv_to_snowflake_table(
    #     'uploads/hired_employees.csv', 'hired_employees', constants.HIRED_EMPLOYEES_DT)

