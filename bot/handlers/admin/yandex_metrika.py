# bot/handlers/admin/yandex_metrika.py

import logging
import secrets
from aiogram import Router, F, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n
from bot.keyboards.inline.admin_keyboards import get_back_to_admin_panel_keyboard
from db.dal import yandex_tracking_dal, user_dal, payment_dal
from bot.services.yandex_metrika_service import YandexMetrikaService

router = Router(name="admin_yandex_metrika_router")


@router.message(Command("yandex_stats"))
async def yandex_stats_command(message: types.Message, settings: Settings, i18n_data: dict, session: AsyncSession):
    """Расширенная статистика по Yandex отслеживанию"""
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: JsonI18n = i18n_data.get("i18n_instance")
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем username бота
        bot_info = await message.bot.get_me()
        bot_username = bot_info.username or "unknown_bot"
        
        metrika_service = YandexMetrikaService(settings, bot_username)
        stats = await metrika_service.get_tracking_statistics(session)
        
        # Форматируем статистику
        stats_text = (
            f"📊 <b>Статистика Yandex.Метрика</b>\n\n"
            f"<b>👥 Пользователи:</b>\n"
            f"├ Всего отслеживаемых: {stats['total_trackings']}\n"
            f"├ С конверсиями: {stats['unique_users_with_conversions']}\n"
            f"└ С множественными визитами: {stats['users_with_multiple_visits']}\n\n"
            
            f"<b>📈 Визиты:</b>\n"
            f"├ Всего визитов: {stats['total_visits']}\n"
            f"├ В среднем на пользователя: {stats['average_visits_per_user']}\n"
            f"└ За последние 24ч: {stats['visits_last_24h']}\n\n"
            
            f"<b>💰 Конверсии:</b>\n"
            f"├ Отправлено: {stats['conversions_sent']}\n"
            f"└ Общая сумма: {stats['total_revenue']:.2f} RUB\n\n"
            
            f"<b>🔧 Настройки:</b>\n"
            f"├ Счетчик: <code>{getattr(settings, 'YANDEX_METRIKA_COUNTER_ID', 'Не настроен')}</code>\n"
            f"└ Токен: {'✅ Настроен' if getattr(settings, 'YANDEX_METRIKA_TOKEN', None) else '❌ Не настроен'}"
        )
        
        await message.answer(
            stats_text, 
            parse_mode="HTML",
            reply_markup=get_back_to_admin_panel_keyboard(current_lang, i18n)
        )
        
    except Exception as e:
        logging.error(f"Error getting Yandex stats: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка получения статистики: {e}")


@router.message(Command("test_yandex"))
async def test_yandex_command(message: types.Message, settings: Settings, session: AsyncSession):
    """Тест отправки тестовой конверсии в Yandex.Метрику"""
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем username бота
        bot_info = await message.bot.get_me()
        bot_username = bot_info.username or "unknown_bot"
        
        metrika_service = YandexMetrikaService(settings, bot_username)
        
        if not metrika_service.configured:
            await message.answer("❌ Yandex.Метрика не настроена")
            return
        
        # Используем тестовый client_id
        test_client_id = ''.join(str(secrets.randbelow(10)) for _ in range(19))
        
        # Отправляем pageview
        pageview_result = await metrika_service.send_pageview(
            client_id=test_client_id,
            page_url=f"https://t.me/{bot_username}",
            page_title="Test Visit"
        )
        
        # Отправляем ecommerce событие
        ecom_result = await metrika_service.send_ecommerce_purchase(
            client_id=test_client_id,
            transaction_id=f"test_{message.message_id}",
            revenue=100.0,
            products=[{
                'id': 'test_subscription',
                'name': 'Test Subscription',
                'brand': 'VPN Service',
                'category': 'Test',
                'price': 100.0,
                'quantity': 1
            }]
        )
        
        # Отправляем конверсию
        conversion_result = await metrika_service.send_conversion(
            client_id=test_client_id,
            goal_name="test_purchase",
            goal_value=100.0
        )
        
        result_text = (
            f"🧪 <b>Тест Yandex.Метрика</b>\n\n"
            f"Client ID: <code>{test_client_id}</code>\n\n"
            f"📄 Pageview: {'✅ Успешно' if pageview_result else '❌ Ошибка'}\n"
            f"🛒 Ecommerce: {'✅ Успешно' if ecom_result else '❌ Ошибка'}\n"
            f"🎯 Conversion: {'✅ Успешно' if conversion_result else '❌ Ошибка'}\n\n"
            f"{'✅ Все тесты пройдены успешно!' if all([pageview_result, ecom_result, conversion_result]) else '⚠️ Есть ошибки в тестах'}"
        )
        
        await message.answer(result_text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error testing Yandex: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка теста: {e}")


@router.message(Command("resend_conversions"))
async def resend_conversions_command(message: types.Message, settings: Settings, session: AsyncSession):
    """Повторная отправка неотправленных конверсий с реальными данными"""
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем username бота
        bot_info = await message.bot.get_me()
        bot_username = bot_info.username or "unknown_bot"
        
        metrika_service = YandexMetrikaService(settings, bot_username)
        
        if not metrika_service.configured:
            await message.answer("❌ Yandex.Метрика не настроена")
            return
        
        # Отправляем начальное сообщение
        status_msg = await message.answer("🔄 Начинаю отправку неотправленных конверсий...")
        
        # Запускаем процесс
        results = await metrika_service.resend_missing_conversions(session, limit=50)
        
        await session.commit()
        
        # Формируем отчет
        result_text = (
            f"✅ <b>Отправка конверсий завершена</b>\n\n"
            f"📊 Обработано пользователей: {results['processed']}\n"
            f"✅ Успешно отправлено: {results['success']}\n"
            f"❌ Ошибок: {results['failed']}\n\n"
        )
        
        if results['failed'] > 0:
            result_text += "⚠️ Некоторые конверсии не удалось отправить. Проверьте логи."
        else:
            result_text += "✨ Все конверсии отправлены успешно!"
        
        # Обновляем сообщение
        await status_msg.edit_text(result_text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error resending conversions: {e}", exc_info=True)
        await message.answer("❌ Ошибка при отправке конверсий. Проверьте логи.")


@router.message(Command("track_visits"))
async def track_visits_command(message: types.Message, settings: Settings, session: AsyncSession):
    """Показывает информацию о визитах пользователей"""
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем пользователей с наибольшим количеством визитов
        from sqlalchemy import select
        from db.models import YandexTracking
        
        stmt = (
            select(YandexTracking)
            .order_by(YandexTracking.visit_count.desc())
            .limit(10)
        )
        result = await session.execute(stmt)
        top_visitors = result.scalars().all()
        
        if not top_visitors:
            await message.answer("📊 Нет данных о визитах")
            return
        
        visits_text = "👥 <b>Топ пользователей по визитам:</b>\n\n"
        
        for idx, tracking in enumerate(top_visitors, 1):
            # Получаем информацию о пользователе
            user = await user_dal.get_user_by_id(session, tracking.user_id)
            user_display = f"@{user.username}" if user and user.username else f"ID:{tracking.user_id}"
            
            # Рассчитываем время с последнего визита
            if tracking.last_visit_time:
                time_since = datetime.now(timezone.utc) - tracking.last_visit_time
                hours_ago = time_since.total_seconds() / 3600
                time_display = f"{hours_ago:.1f}ч назад" if hours_ago < 24 else f"{int(hours_ago/24)}д назад"
            else:
                time_display = "неизвестно"
            
            visits_text += (
                f"{idx}. {user_display}\n"
                f"   📈 Визитов: {tracking.visit_count}\n"
                f"   ⏱ Последний: {time_display}\n\n"
            )
        
        await message.answer(visits_text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error getting visit tracking: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")


@router.message(Command("cleanup_yandex"))
async def cleanup_yandex_command(message: types.Message, settings: Settings, session: AsyncSession):
    """Очистка старых записей отслеживания без конверсий"""
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Удаляем записи старше 30 дней без конверсий
        deleted_count = await yandex_tracking_dal.cleanup_old_tracking(session, days=30)
        await session.commit()
        
        await message.answer(
            f"🗑 Очистка завершена\n"
            f"Удалено старых записей: {deleted_count}"
        )
        
    except Exception as e:
        logging.error(f"Error cleaning up Yandex tracking: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка очистки: {e}")