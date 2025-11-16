""" Execute this file to run frontend """

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import core.config as config
from core.logger import logger
import webbrowser
import threading
import uvicorn
from pathlib import Path

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
pages_dir = Path("pages")
templates = Jinja2Templates(directory=pages_dir)


def scan_and_register_pages():
    """ Scans pages directory and registers HTML pages """

    if not pages_dir.exists():
        logger.error(f"Directory '{pages_dir}' not found")
        return []

    html_files = list(pages_dir.glob("*.html"))

    logger.info(f"Scanning '{pages_dir}': found {len(html_files)} HTML-files")

    registered_pages = []

    for html_file in html_files:
        filename = html_file.name
        route_path = f"/{html_file.stem}"

        if filename == "index.html":
            route_path = "/"

        @app.get(route_path, response_class=HTMLResponse)
        async def page_handler(request: Request, template_file=filename):
            """ Generic page """

            return templates.TemplateResponse(template_file, {"request": request})

        registered_pages.append((route_path, filename))
        logger.debug(f"Route registered: {route_path} -> {filename}")

    logger.info("Scan results:")
    for route, template in registered_pages:
        logger.info(f"{route} -> {template}")

    return registered_pages


@app.exception_handler(404)
async def page_404(request, __):
    """ Pretty error 404 page """

    logger.info(f"404 status for: {request.url.path}")
    return templates.TemplateResponse("page404.html", {"request": request})


def start_server():
    """ Starts the server """

    logger.info(f"Server started at http://127.0.0.1:{config.FRONTEND_PORT}")
    uvicorn.run(app, host="127.0.0.1", port=config.FRONTEND_PORT, reload=False)


def run():
    """ Starts the server """

    logger.info("Scanning for pages...")
    registered_pages = scan_and_register_pages()

    if not registered_pages:
        logger.warning("No pages registered")
    else:
        logger.info(f"Pages registered: {len(registered_pages)}")

    server_thread = threading.Thread(target=start_server, daemon=True)
    server_thread.start()

    url = f"http://127.0.0.1:{config.FRONTEND_PORT}"
    logger.info(f"Opening browser: {url}")
    webbrowser.open(url)

    server_thread.join()


if __name__ == '__main__':
    run()
