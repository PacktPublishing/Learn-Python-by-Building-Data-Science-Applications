from fastapi import APIRouter
from starlette.responses import HTMLResponse
from vdom.helpers import b, div, h1, img, p, span
import matplotlib.pyplot as plt
import io, base64, urllib
import pandas as pd

df = pd.read_csv("./data/2019-06-14.csv")

image_url = "https://pbs.twimg.com/profile_images/775676979655929856/jn13Vq3D.jpg"

router = APIRouter()


def topten():
    fig, ax = plt.subplots()
    df["complaint_type"].value_counts().head(10).plot(kind="barh", ax=ax)
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    buf.seek(0)

    string = base64.b64encode(buf.read())
    plt.close()

    return img(src="data:image/png;base64," + urllib.parse.quote(string))


@router.get("/dashboard", tags=["dashboard"])
async def get_dashboard():
    example = div(
        span(
            img(src=image_url, style=dict(width="100", heigth="100")),
            h1("Smart 311 Dashboard"),
        ),
        p("Written in Python, 100%"),
        topten(),
        p("Top ten complaint types"),
    ).to_html()

    return HTMLResponse(content=example)
