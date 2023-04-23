import logging
import os
from hrpkg import data_operations
import config
import constants
from flask import Flask, request, render_template


logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=os.environ.get("LOGLEVEL", "INFO"))
app = Flask(__name__)


@app.route("/upload_records/<entity>", methods=['GET', 'POST'])
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


if __name__ == "__main__":
    snow_op = data_operations.SnowflakeOperations(config.credentials)
    app.run(host="0.0.0.0", port=3000, debug=True)
