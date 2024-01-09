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
        last_id (str): Идентификатор последнего значения на странице
        client_id (str): Идентификатор клиента
        seller_token (str): API токен маркетплейса

    Returns:
        dict: Список товаров

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> get_product_list("", "12345", "access_token")
        {
            'offerMappingEntries': [...],
            'paging': {...}
        }

        >>> get_product_list("invalid_page", "12345", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
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
    """Получить артикулы товаров магазина озон.

    Args:
        client_id (str): Идентификатор клиента
        seller_token (str): API токен маркетплейса

    Returns:
        list: Артикулы товаров

    Examples:
        >>> get_offer_ids("12345", "access_token")
        []

        >>> ("12345", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
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
    """Устанавливает цены на товары в магазине.

    Args:
        prices (list): Цены товаров
        client_id (str): Идентификатор клиента
        seller_token (str): API токен маркетплейса

    Returns:
        dict: Ответ от маркетплейса

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> update_price(some_price_list, "12345", "access_token")
        {
            "result": [...]
        }

        >>> update_price(some_price_list, "12345", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
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
    """Передает информацию по остаткам.

    Args:
        stocks (list): Артикулы и количество оставших товаров
        client_id (str): Идентификатор клиента
        seller_token (str): API токен маркетплейса

    Returns:
        dict: Ответ от маркетплейса

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> update_stocks(some_stock_list, "12345", "access_token")
        {
            "result": [...]
        }

        >>> update_stocks(some_stock_list, "12345", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
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
    """Скачать остатки товаров с сайта casio.

    Returns:
        list: Остатки товаров

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> download_stock()
        [{'Код': '', 'Наименование товара': '',..},..]

        >>> download_stock()
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
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
    """Получить артикулы и количество товаров.

    Args:
        watch_remnants (list): Остатки товаров
        offer_ids (list): Артикулы товаров

    Returns:
        list: Артикулы и количество оставшихся товаров

    Examples:
        >>> create_stocks(watch_remnants, offer_ids)
        [{"offer_id": offer_id, "stock": 0},..]
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
    """Получить цены товаров.

    Args:
        watch_remnants (list): Остатки товаров
        offer_ids (list): Артикул товаров

    Returns:
        list: Цены товаров

    Examples:
        >>> create_prices(watch_remnants, offer_ids)
        [{"auto_action_enabled": "",..},..]
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
    """Преобразовать цену в целое число в формате строки.

    Args:
        price (str): Цена

    Returns:
        str: Цена в виде целого числа

    Examples:
        >>> price("5'990.00 руб.")
        5990
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Args:
        lst (list): Список
        n (int): Число элементов в одной части списка

    Yields:
        list: Часть входного списка из n элементов

    Examples:
        >>> divide([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4, 5]]
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновить цены товаров на сайте.

    Args:
        watch_remnants (list): Остатки товаров
        client_id (str): Идентификатор клиента
        seller_token (str): API токен продавца

    Returns:
        list: Список цен товаров

    Examples:
        >>> upload_prices(watch_remnants,  "12345", "access_token")
        [{"auto_action_enabled": "",..},..]

        >>> upload_prices(watch_remnants, "12345", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновить отстатки товаров на сайте.
    Args:
        watch_remnants (list): Остатки товаров
        client_id (str): Идентификатор клиента
        seller_token (str): Ключ продавца

    Returns:
        not_empty (list): Товары в наличии
        stocks (list): Информация об остатках товаров

    Examples:
        >>> upload_stocks(watch_remnants, "12345", "access_token")
        [{"offer_id": offer_id, "stock": 0},..], [{},..]

        >>> upload_stocks(watch_remnants, "12345", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
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
