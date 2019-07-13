import requests as rq
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup
import warnings


def _default_collect(url):
    """returns document from url as a BS DOM"""
    response = rq.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")


def _table_to_dict(table):
    """convert table with 2 columns as 
    a dictionery"""
    result = {}
    for row in table.find_all("tr"):
        key = next(row.th.stripped_strings)
        value = row.td.get_text().strip()

        result[key] = value

    return result


def _get_main_info(table):
    """finds "main" data table on the page
    and returns key data points as a dict"""
    main = [
        el
        for el in table.tbody.find_all("tr", recursive=False)
        if "Location" in el.get_text()
    ][0]

    return _table_to_dict(main)


def _parse_row(row, names=("allies", "axis", "third party")):
    """parse secondory info row
    as dict of info points
    """
    cells = row.find_all("td", recursive=False)
    if len(cells) == 1:
        return {"total": cells[0].get_text(separator=" ").strip()}

    return {
        name: cell.get_text(separator=" ").strip() for name, cell in zip(names, cells)
    }


def _find_row_by_header(table, string):
    """find a header row in the table,
    and return NEXT element if finds"""
    header = table.tbody.find("tr", text=string)

    if header:
        return header.next_sibling


def _additional(table):
    """collects additional info
    using header keywords and returning
    data from the row below each
    """

    keywords = (
        "Belligerents",
        "Commanders and leaders",
        "Strength",
        "Casualties and losses",
    )

    result = {}
    for keyword in keywords:
        try:
            data = _find_row_by_header(table, keyword)
            if data:
                result[keyword] = _parse_row(data)
        except Exception as e:
            raise Exception(keyword, e)

    return result


def parse_battle_page(url):
    """ main function to parse battle urls from wikipedia
    """
    try:
        dom = _default_collect(url)  # dom
    except Exception as e:
        warnings.warn(str(e))
        return {}

    table = dom.find("table", "infobox vevent")  # info table
    if table is None:  # some campaigns don't have table
        return {}

    data = _get_main_info(table)
    data["url"] = url

    additional = _additional(table)
    data.update(additional)
    return data
