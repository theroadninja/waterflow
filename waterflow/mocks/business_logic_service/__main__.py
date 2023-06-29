from waterflow.mocks.business_logic_service.biz_logic_flask import app

if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0", port=8080, debug=True)