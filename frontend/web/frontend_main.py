""" Execute this file to run frontend """

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Query, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import core.config as config
import webbrowser
import threading
import uvicorn

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="pages")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """ Main page """

    return templates.TemplateResponse("index.html", {"request": request})


@app.exception_handler(404)
async def page_404(request, __):
    """ Pretty error 404 page """

    return templates.TemplateResponse("page404.html", {"request": request})


def start_server():
    """ Запускает сервер """

    uvicorn.run(app, host="127.0.0.1", port=config.FRONTEND_PORT, reload=False)


def run():
    """ Starts the server """

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()
    webbrowser.open(f"http://127.0.0.1:{config.FRONTEND_PORT}")
    server_thread.join()


if __name__ == '__main__':
    run()
