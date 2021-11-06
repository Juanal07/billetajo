from flask import Flask, jsonify, abort, request, make_response
# from flask_cors import CORS

def main():
    app = Flask(__name__)
    # CORS(app)

    @app.errorhandler(404)
    def not_found(error):
        return make_response(jsonify({'error': 'Not found'}), 404)

    @app.route("/")
    def hello_world():
        return "<p>Hello, World!</p>"

    @app.route('/api', methods=['GET'])
    def test():
        return make_response(jsonify({'test': 'hola mundo!'}), 200)

    # @app.route('/api/sentiment', methods=['POST'])
    # def sentimiento():
    #     return sentiment.getSentiment(request.json['text'])

    app.run(debug=True)

if __name__ == '__main__':

    print('hola')
    # main()
