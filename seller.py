import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Args:
        last_id (str): Идентификатор последнего ID товара, по умолчанию ''.
        client_id (_type_): ID клиента, загружается из .env.
        seller_token (_type_): Токен продавца, загружается из .env.

    Returns:
        list: Cписок товаров магазина озон.

    Examples:
        >>> get_product_list('', '1234', '5678')
        [{'items': {...}, 'total': {...}}]
        >>> get_product_list('', 'invalid', 'token')
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Args:
        client_id (str): ID клиента, загружается из .env
        seller_token (str): Токен продавца, загружается из .env

    Returns:
        list: список артикулов товаров магазина озон.

    Examples:
        >>> get_offer_ids('1234', '5678')
        ['1', '2', '3', '4', '5']
         >>> get_offer_ids('invalid', 'token')
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновление прайс-листа на сайте озон.

    Args:
        prices (list): сформированный прайс-лист.
        client_id (str): ID клиента, загружается из .env.
        seller_token (str): Токен продавца, загружается из .env.

    Returns:
        list: Ответ сервера о выполнении операции.

    Examples:
        >>> update_price([{"offer_id": "123", "price": "599"}], '123', 'token')
        {'code': 0, 'message': ''}
        >>> update_price([], '1234', 'token')
        {'code': 400, 'message': 'Prices not passed'}
        >>> update_price([{"offer_id": "123"}], 'invalid', 'token')
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

    Args:
        stocks (list): позиция из сформированного ассоримента товаров.
        client_id (str): ID клиента, загружается из .env.
        seller_token (str): Токен продавца, загружается из .env.

    Returns:
        list: Ответ сервера о выполнении операции

    Examples:
        >>> update_stocks([{"offer_id": "123", "stock": 10}], '1234', 'token')
        {'code': 0, 'message': ''}
        >>> update_stocks([], '1234', 'token')
        {'code': 400, 'message': 'Stocks not passed'}
        >>> update_stocks([{"offer_id": "123"}], 'invalid', 'token')
        Traceback (most recent call last):
        ...
        requests.exceptions.HTTPError: 401 Client Error: Unauthorized
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.

    Returns:
        list: список остатков часов.

    Examples:
        >>> download_stock()
        [{'Код': 69791, 'Наименование товара': 'Украшение для дисплеев 219RU',
        'Изображение': 'Показать', 'Цена': '550.00 руб.',
        'Количество': '>10', 'Заказ': ''}]
         >>> download_stock()
        Traceback (most recent call last):
        ...
        requests.exceptions.ConnectionError: Failed to establish a new conn...
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Сформировать текущий ассортимент товаров.

    Args:
        watch_remnants (list): список остатков часов.
        offer_ids (list): список артикулов товаров магазина озон.

    Returns:
        list: сформированный ассортимент товаров.

    Examples:
        >>> create_stocks([{'Код': '123', 'Количество': '>10'}], ['123'])
        [{'offer_id': '123', 'stock': 100}]
        >>> create_stocks([{'Код': '123', 'Количество': '>10'}], [])
        []
        >>> create_stocks(None, ['123'])
        Traceback (most recent call last):
        ...
        TypeError: 'NoneType' object is not iterable
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """формирование прайс-листа

    Args:
        watch_remnants (list): список остатков часов.
        offer_ids (list): список артикулов товаров магазина озон.

    Returns:
        list: сформированный прайс-лист.

    Examples:
        >>> create_prices([{'Код': '123', 'Цена': "5'990.00 руб."}], ['123'])
        [{'offer_id': '123', 'price': '5900', ...}]
        >>> create_prices(None, ['123'])
        Traceback (most recent call last):
        ...
        TypeError: 'NoneType' object is not iterable
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену. Пример: 5'990.00 руб. -> 5990.

    Args:
        price (str): Формат цены который нужно преобразовать.

    Returns:
        str: Выходной формат цены.

    Examples:
        >>> price_conversion("5'990.00 руб.")
        5900
        >>> price_conversion(123)
        Traceback (most recent call last):
        ...
        AttributeError: 'int' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

    Args:
        lst (list): список который необходимо разделить на части.
        n (int): делитель для списка, количество элементов.

    Yields:
        list: разделенный список по n-элементов.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
        >>> list(divide([], 2))
        []
        >>> list(divide([1, 2, 3], 0))
        Traceback (most recent call last):
        ...
        ZeroDivisionError: division by zero
    """
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
