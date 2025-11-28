import requests
import re
from typing import Optional, Dict
from logger import setup_logger

logger = setup_logger(__name__)


class MVideoParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
        })

    def parse_product(self, url: str) -> Optional[Dict]:
        try:
            logger.info(f"Начало парсинга товара по URL: {url}")
            main_response = self.session.get(url, timeout=15)
            main_response.raise_for_status()
            return self._parse_via_api_only(url)
        except Exception as e:
            logger.error(f"Ошибка при парсинге товара: {e}")
            return None

    def _parse_via_api_only(self, url: str) -> Optional[Dict]:
        try:
            product_id_match = re.search(r'products/([^/?]+)-(\d+)', url)
            if not product_id_match:
                logger.warning(f"Не удалось извлечь productId из URL: {url}")
                return None

            product_id = product_id_match.group(2)
            logger.info(f"Извлечен productId: {product_id}")

            product_data = self._get_api_data(product_id, url)
            if not product_data:
                return None

            price_data = self._get_price_data(product_id)
            price = self._extract_price_from_api(price_data)

            description = product_data.get('description', '')
            if description:
                description = description.replace('\u003Cbr', '').replace('\u003E', '')

            result = {
                "name": product_data.get('name', ''),
                "price": price,
                "description": description,
            }

            logger.info(f"Данные товара получены: название='{result['name']}', цена={price}")
            return result

        except Exception as e:
            logger.error(f"Ошибка при API парсинге: {e}")
            return None

    def _get_api_data(self, product_id: str, referer_url: str) -> Optional[Dict]:
        try:
            api_url = f"https://www.mvideo.ru/bff/product-details?productId={product_id}"

            api_headers = {
                'Accept': 'application/json, text/plain, */*',
                'Referer': referer_url,
                'Origin': 'https://www.mvideo.ru',
            }

            logger.info(f"Запрос данных товара через API для productId: {product_id}")
            response = self.session.get(api_url, headers=api_headers, timeout=15)
            logger.info(f"Статус ответа API данных товара: {response.status_code}")

            if response.status_code != 200:
                logger.warning(f"API вернул неожиданный статус: {response.status_code}")
                return None

            data = response.json()
            logger.info("Данные товара успешно получены через API")
            return data.get('body', {})

        except Exception as e:
            logger.error(f"Ошибка при получении API данных товара: {e}")
            return None

    def _get_price_data(self, product_id: str) -> Optional[Dict]:
        try:
            price_url = f"https://www.mvideo.ru/bff/products/prices?productIds={product_id}&addBonusRubles=true&isPromoApplied=true"

            response = self.session.get(price_url, timeout=15)
            if response.status_code == 200:
                data = response.json()
                logger.info("Данные о ценах успешно получены")
                return data
            else:
                logger.warning(f"Ошибка при получении данных о ценах, статус: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при получении данных о ценах: {e}")
            return None

    def _extract_price_from_api(self, price_data: Optional[Dict]) -> Optional[str]:
        if not price_data:
            logger.warning("Нет данных о ценах для извлечения")
            return None

        try:
            path = ['body', 'materialPrices', 0, 'price', 'salePrice']
            value = price_data
            for key in path:
                if isinstance(value, list) and isinstance(key, int) and key < len(value):
                    value = value[key]
                elif isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    value = None
                    break
                if value and isinstance(value, (int, float)):
                    formatted_price = f"{int(value):,} ₽".replace(',', ' ')
                    logger.info(f"Цена успешно извлечена: {formatted_price}")
                    return formatted_price

            logger.warning("Не удалось извлечь цену по указанному пути")
            return None
        except Exception as e:
            logger.error(f"Ошибка при извлечении цены из API: {e}")
            return None

    def _clean_price(self, price_text: str) -> str:
        cleaned = re.sub(r'[^\d\s]', '', price_text.strip())
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return f"{cleaned} ₽"


def parse_mvideo_product(url: str) -> Optional[Dict]:
    logger.info(f"Запуск парсинга товара: {url}")
    parser = MVideoParser()
    result = parser.parse_product(url)

    if result:
        logger.info(f"Парсинг завершен успешно для URL: {url}")
    else:
        logger.warning(f"Парсинг завершен с ошибкой для URL: {url}")

    return result