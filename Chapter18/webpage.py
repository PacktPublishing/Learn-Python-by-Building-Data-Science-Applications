from fastapi import APIRouter
from starlette.responses import HTMLResponse
from vdom.helpers import b, div, h1, img, p, span
router = APIRouter()

image_url = "https://pbs.twimg.com/profile_images/775676979655929856/jn13Vq3D.jpg"
from vdom.helpers import b, div, h1, img, p, span

def dashboard():
    return div(
        span(img(src=image_url, style=dict(width='100', heigth='100')), 
             h1('Smart 311 Dashboard')),
        p('Written in Python, 100%')
    )


@router.get('/dashboard', tags=["dashboard"])
def get_dashboard():
    content = dashboard().to_html()
    return HTMLResponse(content=content)
