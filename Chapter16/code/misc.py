import time
from wikiwwii.collect.battles import parse_battle_page


def _parse_in_depth(element, name):
    """attempts to scrape data for every
    element with url attribute - and all the children
    if there are any"""

    if "children" in element:
        for k, child in element["children"].items():
            parsed = _parse_in_depth(child, k)
            element["children"][k].update(parsed)

    if element.get("url", None):
        try:
            element.update(parse_battle_page(element["url"]))
        except Exception as e:
            raise Exception(name, e)

    time.sleep(0.1)  # let's be good citizens!
    return element


def _flattn_depth(element, name="unknown"):
    """generate a flat list of battles to"""
    results = []

    if "children" in element:
        for k, child in element["children"].items():
            results.expand(_flattn_depth(child, k))

    if "url" in element:
        results.append(element)

    return results
