import logging
import secrets
from aiogram import Router, F, types
from aiogram.filters import Command
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from bot.middlewares.i18n import JsonI18n
from bot.keyboards.inline.admin_keyboards import get_back_to_admin_panel_keyboard
from db.dal import yandex_tracking_dal, user_dal
from bot.services.yandex_metrika_service import YandexMetrikaService

router = Router(name="admin_yandex_metrika_router")


@router.message(Command("yandex_stats"))
async def yandex_stats_command(message: types.Message, settings: Settings, i18n_data: dict, session: AsyncSession):
    """Статистика по Yandex отслеживанию"""
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: JsonI18n = i18n_data.get("i18n_instance")
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        stats = await yandex_tracking_dal.get_tracking_statistics(session)
        
        stats_text = (
            f"📊 <b>Статистика Yandex.Метрика</b>\n\n"
            f"👥 Всего отслеживаемых: {stats['total_trackings']}\n"
            f"✅ Конверсий отправлено: {stats['conversions_sent']}\n"
            f"⏳ Конверсий в ожидании: {stats['conversions_pending']}\n\n"
            f"🔧 Настройки:\n"
            f"Счетчик: {getattr(settings, 'YANDEX_METRIKA_COUNTER_ID', 'Не настроен')}\n"
            f"Токен: {'Настроен' if getattr(settings, 'YANDEX_METRIKA_TOKEN', None) else 'Не настроен'}"
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
        bot_username = "unknown_bot"
        try:
            bot_info = await message.bot.get_me()
            bot_username = bot_info.username or "unknown_bot"
        except Exception:
            pass
        
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
        
        # Отправляем конверсию
        conversion_result = await metrika_service.send_conversion(
            client_id=test_client_id,
            goal_name="test_purchase",
            goal_value=100.0
        )
        
        result_text = (
            f"🧪 <b>Тест Yandex.Метрика</b>\n\n"
            f"Client ID: <code>{test_client_id}</code>\n"
            f"📄 Pageview: {'✅' if pageview_result else '❌'}\n"
            f"🎯 Conversion: {'✅' if conversion_result else '❌'}\n\n"
            f"{'✅ Тест успешен!' if pageview_result and conversion_result else '❌ Есть ошибки в тесте'}"
        )
        
        await message.answer(result_text, parse_mode="HTML")
        
    except Exception as e:
        logging.error(f"Error testing Yandex: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка теста: {e}")


@router.message(Command("resend_conversions"))
async def resend_conversions_command(message: types.Message, settings: Settings, session: AsyncSession):
    """Повторная отправка неотправленных конверсий"""
    
    if message.from_user.id not in settings.ADMIN_IDS:
        return
    
    try:
        # Получаем username бота
        bot_username = "unknown_bot"
        try:
            bot_info = await message.bot.get_me()
            bot_username = bot_info.username or "unknown_bot"
        except Exception:
            pass
        
        metrika_service = YandexMetrikaService(settings, bot_username)
        
        if not metrika_service.configured:
            await message.answer("❌ Yandex.Метрика не настроена")
            return
        
        # Получаем пользователей с неотправленными конверсиями
        pending_trackings = await yandex_tracking_dal.get_users_without_conversion(session, limit=50)
        
        if not pending_trackings:
            await message.answer("✅ Нет неотправленных конверсий")
            return
        
        await message.answer(f"🔄 Начинаю отправку {len(pending_trackings)} конверсий...")
        
        success_count = 0
        for tracking in pending_trackings:
            try:
                # Отправляем конверсию (используем базовые данные)
                success = await metrika_service.send_full_conversion_chain(
                    session=session,
                    user_id=tracking.user_id,
                    payment_amount=100.0,  # Примерная сумма
                    payment_id=f"resend_{tracking.tracking_id}",
                    subscription_months=1
                )
                
                if success:
                    success_count += 1
                    
            except Exception as e:
                logging.error(f"Failed to resend conversion for user {tracking.user_id}: {e}")
        
        await session.commit()
        
        result_text = f"✅ Отправлено {success_count} из {len(pending_trackings)} конверсий"
        await message.answer(result_text)
        
    except Exception as e:
        logging.error(f"Error resending conversions: {e}", exc_info=True)
        await message.answer(f"❌ Ошибка: {e}")
