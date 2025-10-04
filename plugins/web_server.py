from aiohttp import web
from config import *

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(
        text="Bot is Running",
        content_type="text/html"
    )

async def web_server():
    web_app = web.Application()
    web_app.add_routes(routes)
    return web_app
