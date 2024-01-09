import datetime
import logging.config
import requests
from environs import Env
from seller import download_stock
from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров яндекс маркета.

    Args:
        page (str): Идентификатор страницы каталога
        campaign_id (str): Идентификатор магазина, в котором размещен товар
        access_token (str): API токен маркетплейса

    Returns:
        dict: Список товаров

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> get_product_list("", "1234", "access_token")
        {
            "paging": {...},
            "offerMappingEntries": [...],
            ...
        }

        >>> get_product_list("", "1234", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Передает информацию по остаткам.

    Args:
        stocks (list): Артикулы и количество оставшихся товаров
        campaign_id (str): Идентификатор магазина, в котором размещен товар
        access_token (str): API токен маркетплейса

    Returns:
        dict: Ответ от маркетплейса

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> update_stocks(some_stock, "1234", "access_token")
        {
            "offers": [...]
        }

        >>> update_stocks(some_stock, "1234", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Устанавливает цены на товары в магазине.

    Args:
        prices (list): Цены товаров
        campaign_id (str): Идентификатор магазина, в котором размещен товар
        access_token (str): API токен маркетплейса

    Returns:
        dict: Ответ от маркетплейса

    Raises:
        requests.exceptions.HTTPError

    Examples:
        >>> update_price(prices, "1234", "access_token")
        {
            "offers": [...]
        }

        >>> update_price(prices, "1234", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета.

    Args:
        campaign_id (str): Идентификатор магазина, в котором размещен товар
        market_token (str): API токен маркетплейса

    Returns:
        list: Артикулы товаров

    Examples:
        >>> get_offer_ids("1234", "access_token")
        [123,..]

        >>> get_offer_ids("1234", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Получить артикулы и количество товаров.

    Args:
        watch_remnants (list): Остатки товаров
        offer_ids (list): Артикулы товаров
        warehouse_id (str): Идентификатор склада

    Returns:
        list: Артикулы и количество оставшихся товаров

    Examples:
        >>> create_stocks(watch_remnants, offer_ids, "123")
        [{"sku": "", "warehouseId": "", "items": [...]}}]
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(
        microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
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
        ["id": "", "price": {...}]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Обновить цены товаров на сайте.

    Args:
        watch_remnants (list): Остатки товаров
        client_id (str): Идентификатор клиента
        seller_token (str): API токен продавца

    Returns:
        list: Цены товаров

    Examples:
        >>> upload_prices(watch_remnants, "1234", "access_token")
        ["id": "", "price": {...}]

        >>> upload_prices(watch_remnants, "1234", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token,
                        warehouse_id):
    """Обновить отстатки товаров на сайте.

    Args:
        watch_remnants (list): Остатки товаров
        campaign_id (str): Идентификатор клиента
        market_token (str): API токен маркетплейса
        warehouse_id (str): Идентификатор склада

    Returns:
        not_empty (list): Товары в наличии
        stocks (list): Информация об остатках товаров

    Examples:
        >>> upload_stocks(watch_remnants, "1234", "access_token")
        [{"sku": "", "warehouseId": "", "items": [...]}}], [{..}]

        >>> upload_stocks(watch_remnants, "1234", "invalid_token")
        Traceback (most recent call last):
        ...
        requests.exceptions.RequestException
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
