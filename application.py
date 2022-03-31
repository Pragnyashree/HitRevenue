from flask import Flask, render_template, request, send_file
from modules.DataTransformation import DataTransformation
import os

application = Flask(__name__)


@application.route("/")
def hello():
    return render_template('index.html')


@application.route("/upload_file", methods=['GET', 'POST'])
def upload_file():
    try:
        if request.method == 'POST':  # check if the method is post
            f = request.files['file']  # get the file from form object
            f.save(os.path.join(application.root_path, 'upload/' + f.filename))
            inst = DataTransformation()
            output_filename = inst.generate_output(os.path.join(application.root_path
                                                                , 'upload/' + f.filename), application.root_path)
            return send_file(os.path.join(application.root_path, 'upload/' + output_filename),
                             attachment_filename=output_filename, as_attachment=True)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    application.debug = True
    application.run()
