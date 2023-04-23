import logging
import os
from hrpkg import data_operations
import config
import constants
from flask import Flask, request, send_file


logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=os.environ.get("LOGLEVEL", "INFO"))
app = Flask(__name__)

"""
This route receives a file, uploads it to the upload folder and then creates
a table with the given name on Snowflake
The name of the table is a parameter on the URL string.
"""

@app.route("/upload_data/<entity>", methods=['GET', 'POST'])
def manage_upload(entity):
    if entity in constants.VALID_ENTITIES:
        if request.method == 'POST':
            uploaded_file = request.files['file']
            if uploaded_file.filename != '':
                uploaded_file.save(f'uploads/{entity}.csv')
                bad_rows = snow_op.load_csv_to_snowflake_table(
                    f'uploads/{entity}.csv', f'{entity}', constants.DICT_DT[entity])
                if bad_rows.shape[0] > 0:
                    return f"""
                        <h2>These records were not proccesed dute to rule violation<h2>
                        {bad_rows.to_html()}
                        """
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
    return "<h2>Not valid entity name<h2>"

"""
This route retrieves data from a Snowflake table a exports it to an avro file that can be downloaded.
The name of the table is a parameter on the URL string.
"""
@app.route("/backup/<entity>", methods=['GET'])
def manage_backup(entity):
    if entity in constants.VALID_ENTITIES:
        snow_op.export_to_avro(entity, constants.AVRO_SCHEMA[entity])
        return send_file(f'output_files/{entity}.avro', as_attachment=True)

"""
This route restores a table with a given name on Snowflake. It receives an avro file. The name of the table is a parameter on the URL string.
"""
@app.route("/restore/<entity>", methods=['GET', 'POST'])
def manage_restore(entity):
    if entity in constants.VALID_ENTITIES:
        if request.method == 'POST':
            uploaded_file = request.files['file']
            if uploaded_file.filename != '':
                uploaded_file.save(f'uploads/{entity}.avro')
                snow_op.restore_table_from_avro(entity)
            return f"<h2>File was uploaded and table '{entity}' was restored succesfully<h2>"
        return f'''
            <!doctype html>
            <title>Restore</title>
            <h1>Upload The file for restoring: {entity}</h1>
            <form method=post enctype=multipart/form-data>
            <input type=file name=file>
            <input type=submit value=Upload>
            </form>
        '''
    return "<h2>Not valid entity name<h2>"


if __name__ == "__main__":
    snow_op = data_operations.SnowflakeOperations(config.credentials)
    app.run(host="0.0.0.0", port=3000, debug=True)
