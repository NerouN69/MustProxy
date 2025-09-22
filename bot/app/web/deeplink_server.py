import logging
import asyncio
import os
from typing import Optional
from aiohttp import web, ClientSession
from aiohttp.web_response import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
from string import Template

from config.settings import Settings
from db.dal import yandex_tracking_dal

class DeeplinkServer:
    """Веб-сервер для обработки диплинков с Яндекс.Директа"""

    def __init__(self, settings: Settings, session_factory: sessionmaker, bot_username: str):
        self.settings = settings
        self.session_factory = session_factory
        self.bot_username = bot_username
        self.app = web.Application(middlewares=[self.handle_bad_requests])
        self._setup_routes()

    @web.middleware
    async def handle_bad_requests(self, request: web.Request, handler):
        """Middleware для обработки некорректных запросов"""
        try:
            return await handler(request)
        except web.HTTPBadRequest as e:
            logging.warning(f"Bad request from {request.remote}: {e}")
            return web.json_response({"error": "Invalid request format"}, status=400)
        except web.HTTPNotFound as e:
            logging.warning(f"Not found request from {request.remote}: {e}")
            return web.json_response({"error": "Resource not found"}, status=404)
        except Exception as e:
            logging.error(f"Unexpected error from {request.remote}: {e}", exc_info=True)
            return web.json_response({"error": "Internal server error"}, status=500)

    def _setup_routes(self):
        """Настройка маршрутов"""
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_post('/api/track', self.handle_track)
        self.app.router.add_get('/health', self.handle_health)
        self.app.router.add_get('/robots.txt', self.handle_robots_txt)  # Новый маршрут для robots.txt

    async def handle_robots_txt(self, request: web.Request) -> Response:
        """Обработчик для robots.txt, запрещающий индексацию"""
        robots_content = """User-agent: *
Disallow: /
"""
        return Response(text=robots_content, content_type='text/plain')

    async def handle_index(self, request: web.Request) -> Response:
        """Главная страница с диплинком"""
        client_id = request.query.get('yclid') or request.query.get('client_id')

        html_template = Template("""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Переход в Telegram бот</title>
    <meta name="robots" content="noindex, nofollow">  <!-- Запрет индексации страницы -->
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            color: white;
        }

        .container {
            text-align: center;
            background: rgba(255, 255, 255, 0.1);
            padding: 3rem;
            border-radius: 20px;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
            backdrop-filter: blur(10px);
            border: 1px solid rgba(255, 255, 255, 0.2);
            max-width: 500px;
        }

        .logo {
            font-size: 4rem;
            margin-bottom: 1rem;
            filter: drop-shadow(0 4px 8px rgba(0, 0, 0, 0.2));
        }

        h1 {
            font-size: 2rem;
            margin-bottom: 1rem;
            font-weight: 300;
        }

        p {
            font-size: 1.1rem;
            margin-bottom: 2rem;
            opacity: 0.9;
            line-height: 1.6;
        }

        .telegram-btn {
            display: inline-block;
            background: #0088cc;
            color: white;
            text-decoration: none;
            padding: 1rem 2rem;
            border-radius: 50px;
            font-size: 1.1rem;
            font-weight: 500;
            transition: all 0.3s ease;
            box-shadow: 0 10px 25px rgba(0, 136, 204, 0.3);
            border: none;
            cursor: pointer;
        }

        .telegram-btn:hover {
            background: #006699;
            transform: translateY(-2px);
            box-shadow: 0 15px 35px rgba(0, 136, 204, 0.4);
        }

        .error {
            color: #ff6b6b;
            background: rgba(255, 107, 107, 0.1);
            padding: 1rem;
            border-radius: 10px;
            margin: 1rem 0;
            border: 1px solid rgba(255, 107, 107, 0.3);
        }

        .success {
            color: #51cf66;
            background: rgba(81, 207, 102, 0.1);
            padding: 1rem;
            border-radius: 10px;
            margin: 1rem 0;
            border: 1px solid rgba(81, 207, 102, 0.3);
        }

        @media (max-width: 768px) {
            .container {
                margin: 1rem;
                padding: 2rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="logo">🚀</div>
        <h1>Добро пожаловать!</h1>
        <p>Сейчас вы будете перенаправлены в наш Telegram бот для получения подписки.</p>
        <div id="status"></div>
        <button id="telegramBtn" class="telegram-btn" onclick="openTelegram()">
            📱 Открыть в Telegram
        </button>
    </div>

    <script>
        const clientId = '$client_id';
        const botUsername = '$bot_username';

        async function trackUser() {
            if (!clientId) return;
            try {
                const response = await fetch('/api/track', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        client_id: clientId,
                        referrer: document.referrer || 'direct',
                        user_agent: navigator.userAgent
                    })
                });
                if (response.ok) {
                    console.log('Tracking successful');
                } else {
                    console.error('Tracking failed:', response.status);
                }
            } catch (error) {
                console.error('Tracking error:', error);
            }
        }

        function openTelegram() {
            if (!clientId) {
                document.getElementById('status').innerHTML = '<div class="error">❌ Ошибка: не найден идентификатор клиента</div>';
                return;
            }
            const telegramUrl = `https://t.me/$bot_username?start=yandex_$client_id`;
            window.location.href = telegramUrl;
            setTimeout(() => {
                document.getElementById('status').innerHTML = '<div class="success">✅ Переход в Telegram...</div>';
            }, 1000);
        }

        window.onload = function() {
            if (clientId && clientId !== 'None') {
                trackUser();
                document.getElementById('status').innerHTML = '<div class="success">✅ Пользователь отслеживается</div>';
            } else {
                document.getElementById('status').innerHTML = '<div class="error">⚠️ Идентификатор клиента не найден. Отслеживание конверсий будет недоступно.</div>';
            }
        };
    </script>
</body>
</html>""")

        logging.debug("HTML template: %s", html_template.template[:100])

        try:
            html_content = html_template.substitute(
                client_id=client_id or 'None',
                bot_username=self.bot_username
            )
        except KeyError as e:
            logging.error("Template substitution failed: %s", e)
            return web.json_response({"error": "Template rendering error"}, status=500)

        return Response(text=html_content, content_type='text/html')

    async def handle_track(self, request: web.Request) -> Response:
        """Обработка запроса на отслеживание"""
        try:
            data = await request.json()
            client_id = data.get('client_id')

            if not client_id or client_id == 'None':
                return web.json_response(
                    {'error': 'No client_id provided'},
                    status=400
                )

            logging.info(f"Tracking user with client_id: {client_id}")
            return web.json_response({'status': 'success', 'client_id': client_id})

        except ValueError:
            logging.warning(f"Invalid JSON in track request from {request.remote}")
            return web.json_response({'error': 'Invalid JSON format'}, status=400)
        except Exception as e:
            logging.error(f"Error in track handler: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)

    async def handle_health(self, request: web.Request) -> Response:
        """Health check endpoint"""
        return web.json_response({'status': 'healthy'})

    async def start_server(self) -> web.AppRunner:
        """Запуск сервера"""
        runner = web.AppRunner(self.app)
        await runner.setup()

        site = web.TCPSite(
            runner,
            host=self.settings.DEEPLINK_SITE_HOST,
            port=self.settings.DEEPLINK_SITE_PORT
        )
        await site.start()

        logging.info(
            f"Deeplink server started on "
            f"{self.settings.DEEPLINK_SITE_HOST}:{self.settings.DEEPLINK_SITE_PORT}"
        )

        return runner