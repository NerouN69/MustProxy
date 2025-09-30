# bot/handlers/admin/yandex_metrika.py

import logging
import secrets
import asyncio
from aiogram import Router, F, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n
from bot.keyboards.inline.admin_keyboards import get_back_to_admin_panel_keyboard, get_yandex_metrika_menu_keyboard
from db.dal import yandex_tracking_dal, user_dal, payment_dal
from bot.services.yandex_metrika_service import YandexMetrikaService

router = Router(name="admin_yandex_metrika_router")


@router.callback_query(F.data == "admin_action:yandex_metrika")
async def yandex_metrika_menu_handler(callback: types.CallbackQuery, settings: Settings, 
                                      i18n_data: dict, session: AsyncSession):
    """Отображение меню Яндекс.Метрики"""
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: JsonI18n = i18n_data.get("i18n_instance")
    
    if callback.from_user.id not in settings.ADMIN_IDS:
        await callback.answer("Access denied", show_alert=True)
        return
    
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)
    
    try:
        # Получаем краткую статистику
        bot_info = await callback.bot.get_me()
        bot_username = bot_info.username or "unknown_bot"
        metrika_service = YandexMetrikaService(settings, bot_username)
        stats = await metrika_service.get_tracking_statistics(session)
        
        menu_text = (
            f"📊 <b>Яндекс.Метрика</b>\n\n"
            f"👥 Отслеживается: {stats['total_trackings']}\n"
            f"💰 Конверсий: {stats['conversions_sent']}\n"
            f"📈 Визитов (24ч): {stats['last_visit_time']}\n\n"
            f"Выберите действие:"
        )
        
        await callback.message.edit_text(
            menu_text,
            parse_mode="HTML",
            reply_markup=get_yandex_metrika_menu_keyboard(i18n, current_lang)
        )
        
    except Exception as e:
        logging.error(f"Error showing Yandex Metrika menu: {e}")
        await callback.answer("Ошибка загрузки меню", show_alert=True)
    
    await callback.answer()


@router.callback_query(F.data == "yandex_action:stats")
async def yandex_stats_callback(callback: types.CallbackQuery, settings: Settings, 
                                i18n_data: dict, session: AsyncSession):
    """Расширенная статистика по Yandex отслеживанию"""
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: JsonI18n = i18n_data.get("i18n_instance")
    
    if callback.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем username бота
        bot_info = await callback.bot.get_me()
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
        
        await callback.message.answer(
            stats_text, 
            parse_mode="HTML",
            reply_markup=get_back_to_admin_panel_keyboard(current_lang, i18n)
        )
        
    except Exception as e:
        logging.error(f"Error getting Yandex stats: {e}", exc_info=True)
        await callback.message.answer(f"❌ Ошибка получения статистики: {e}")


@router.callback_query(F.data == "yandex_action:test")
async def yandex_test_callback(callback: types.CallbackQuery, settings: Settings, session: AsyncSession):
    """Тест отправки тестовых событий в Yandex.Метрику"""
    
    if callback.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем username бота
        bot_info = await callback.bot.get_me()
        bot_username = bot_info.username or "unknown_bot"
        
        metrika_service = YandexMetrikaService(settings, bot_username)
        
        if not metrika_service.configured:
            await callback.message.answer("❌ Yandex.Метрика не настроена")
            return
        
        # Используем тестовый client_id
        import secrets
        test_client_id = ''.join(str(secrets.randbelow(10)) for _ in range(19))
        test_user_id = callback.from_user.id
        
        # Отправляем событие install
        install_result = await metrika_service.send_install_event(
            session=session,
            user_id=test_user_id,
            client_id=test_client_id
        )
        
        # Небольшая задержка между событиями
        await asyncio.sleep(1)
        
        # Отправляем событие purchase
        purchase_result = await metrika_service.send_purchase_event(
            session=session,
            user_id=test_user_id,
            payment_amount=100.0,
            payment_id=f"test_{callback.message.message_id}"
        )
        
        result_text = (
            f"🧪 <b>Тест Yandex.Метрика</b>\n\n"
            f"Client ID: <code>{test_client_id}</code>\n\n"
            f"📱 Install event: {'✅ Успешно' if install_result else '❌ Ошибка'}\n"
            f"💰 Purchase event: {'✅ Успешно' if purchase_result else '❌ Ошибка'}\n\n"
            f"{'✅ Все тесты пройдены успешно!' if all([install_result, purchase_result]) else '⚠️ Есть ошибки в тестах'}"
        )
        
        await callback.message.answer(result_text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error testing Yandex: {e}", exc_info=True)
        await callback.message.answer(f"❌ Ошибка теста: {e}")


@router.callback_query(F.data == "yandex_action:visits")
async def yandex_visits_callback(callback: types.CallbackQuery, settings: Settings, session: AsyncSession):
    """Показывает информацию о визитах пользователей"""
    
    if callback.from_user.id not in settings.ADMIN_IDS:
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
            await callback.message.answer("📊 Нет данных о визитах")
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
        
        await callback.message.answer(visits_text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error getting visit tracking: {e}", exc_info=True)
        await callback.message.answer(f"❌ Ошибка: {e}")


@router.callback_query(F.data == "yandex_action:cleanup")
async def yandex_cleanup_callback(callback: types.CallbackQuery, settings: Settings, session: AsyncSession):
    """Очистка старых записей отслеживания без конверсий"""
    
    if callback.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Удаляем записи старше 30 дней без конверсий
        deleted_count = await yandex_tracking_dal.cleanup_old_tracking(session, days=30)
        await session.commit()
        
        await callback.message.answer(
            f"🗑 Очистка завершена\n"
            f"Удалено старых записей: {deleted_count}"
        )
        
    except Exception as e:
        logging.error(f"Error cleaning up Yandex tracking: {e}", exc_info=True)
        await callback.message.answer(f"❌ Ошибка очистки: {e}")