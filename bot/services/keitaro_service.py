# bot/services/keitaro_service.py

import logging
import asyncio
import aiohttp
from typing import Optional
from urllib.parse import urlencode


class KeitaroService:
    """Сервис для отправки постбеков в Keitaro трекер"""

    def __init__(self):
        # Базовый URL для постбеков Keitaro
        self.base_url = "https://aljerieyick.beget.app/729b958/postback"
        logging.info("Keitaro service initialized")

    async def send_install_postback(self, subid: str) -> bool:
        """
        Отправляет постбек на Keitaro для события установки (install)

        Args:
            subid: Keitaro subid пользователя

        Returns:
            bool: True если постбек успешно отправлен, False в противном случае
        """
        if not subid:
            logging.warning("Cannot send install postback: subid is empty")
            return False

        params = {
            "subid": subid,
            "status": "lead"
        }

        return await self._send_postback(params, "install")

    async def send_purchase_postback(self, subid: str, payout: float) -> bool:
        """
        Отправляет постбек на Keitaro для события оплаты (purchase)

        Args:
            subid: Keitaro subid пользователя
            payout: Сумма оплаты

        Returns:
            bool: True если постбек успешно отправлен, False в противном случае
        """
        if not subid:
            logging.warning("Cannot send purchase postback: subid is empty")
            return False

        params = {
            "subid": subid,
            "status": "sale",
            "payout": str(payout)
        }

        return await self._send_postback(params, "purchase")

    async def _send_postback(self, params: dict, event_type: str) -> bool:
        """
        Отправляет HTTP-запрос с постбеком на Keitaro

        Args:
            params: Параметры для постбека
            event_type: Тип события (для логирования)

        Returns:
            bool: True если запрос успешен, False в противном случае
        """
        try:
            query_string = urlencode(params)
            url = f"{self.base_url}?{query_string}"

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    success = response.status == 200
                    if success:
                        logging.info(
                            f"Successfully sent {event_type} postback to Keitaro "
                            f"for subid: {params.get('subid', 'unknown')}"
                        )
                    else:
                        response_text = await response.text()
                        logging.error(
                            f"Failed to send {event_type} postback to Keitaro. "
                            f"Status: {response.status}, Response: {response_text}"
                        )
                    return success
        except asyncio.TimeoutError:
            logging.error(f"Timeout sending {event_type} postback to Keitaro")
            return False
        except Exception as e:
            logging.error(f"Error sending {event_type} postback to Keitaro: {e}", exc_info=True)
            return False

    async def close(self):
        """Cleanup method"""
        pass
