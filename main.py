from flask import Flask
from app.data_processed import load_and_process_data
from app.routes import init_routes

def create_app():
    app = Flask(__name__)
    init_routes(app)
    return app

if __name__ == "__main__":
    load_and_process_data()
    app = create_app()
    app.run(port=8080, debug=True)