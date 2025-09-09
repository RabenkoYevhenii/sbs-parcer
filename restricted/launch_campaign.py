#!/usr/bin/env python3
"""
Interactive Campaign Launcher for SBC Summit 2025
Интерактивный запуск кампаний с пошаговым руководством
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

# Добавляем путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from campaign_config import is_working_time, validate_config
    from campaign_manager import CampaignManager
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что все необходимые файлы находятся в папке restricted/")
    sys.exit(1)


class InteractiveCampaignLauncher:
    """Интерактивный запуск кампаний"""

    def __init__(self):
        self.manager = CampaignManager()
        self.current_dir = Path(__file__).parent

    def print_header(self):
        """Выводит заголовок"""
        print("\n" + "=" * 80)
        print("🚀 FIRST WAVE CAMPAIGN - SBC SUMMIT 2025")
        print("   Интерактивный запуск кампаний сообщений")
        print("=" * 80)
        print(f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}")

        # Показываем рабочее время
        if is_working_time():
            print("✅ Рабочее время - можно отправлять сообщения")
        else:
            print("⚠️  Нерабочее время - рекомендуется отложить отправку")

    def check_prerequisites(self) -> bool:
        """Проверяет предварительные условия"""
        print("\n🔍 ПРОВЕРКА ГОТОВНОСТИ СИСТЕМЫ")
        print("-" * 40)

        checks = self.manager.check_campaign_readiness()

        all_good = True

        if checks["csv_exists"]:
            print("✅ CSV файл найден")
        else:
            print("❌ CSV файл не найден (SBC - Attendees.csv)")
            all_good = False

        if checks["csv_readable"]:
            print("✅ CSV файл читается")
        else:
            print("❌ Ошибка чтения CSV файла")
            all_good = False

        if checks["has_target_audience"]:
            print("✅ Найдена целевая аудитория")
        else:
            print("❌ Нет доступных контактов для отправки")
            all_good = False

        if checks["config_valid"]:
            print("✅ Конфигурация валидна")
        else:
            print("❌ Ошибка в конфигурации")
            all_good = False

        # Выводим рекомендации
        if checks["recommendations"]:
            print(f"\n💡 Рекомендации:")
            for rec in checks["recommendations"]:
                print(f"   • {rec}")

        return all_good

    def show_target_audience_preview(self):
        """Показывает превью целевой аудитории"""
        print("\n🎯 АНАЛИЗ ЦЕЛЕВОЙ АУДИТОРИИ")
        print("-" * 40)

        analysis = self.manager.identify_target_audience()
        if "error" in analysis:
            print(f"❌ {analysis['error']}")
            return False

        print(f"📊 Всего доступно контактов: {analysis['total_available']:,}")
        print(f"⭐ Приоритетные контакты: {analysis['priority_contacts']:,}")
        print(f"👥 Обычные контакты: {analysis['regular_contacts']:,}")
        print(f"📋 Связались ранее: {analysis['contacted_previously']:,}")

        if analysis["gaming_verticals"]:
            print(f"\n🎮 ТОП Gaming Verticals:")
            for i, (vertical, count) in enumerate(
                list(analysis["gaming_verticals"].items())[:5], 1
            ):
                print(f"   {i}. {vertical}: {count}")

        if analysis["top_positions"]:
            print(f"\n💼 ТОП Позиции:")
            for i, (position, count) in enumerate(
                list(analysis["top_positions"].items())[:5], 1
            ):
                print(f"   {i}. {position}: {count}")

        return True

    def ask_confirmation(self, message: str, default: bool = False) -> bool:
        """Запрашивает подтверждение у пользователя"""
        default_text = "Y/n" if default else "y/N"
        response = input(f"\n❓ {message} ({default_text}): ").strip().lower()

        if not response:
            return default

        return response in ["y", "yes", "да", "д"]

    def select_campaign_mode(self) -> tuple:
        """Выбор режима кампании"""
        print("\n⚙️ РЕЖИМ КАМПАНИИ")
        print("-" * 40)
        print("1. 🧪 Только тест (3-10 контактов)")
        print("2. 🚀 Полная кампания (тест + основная отправка)")
        print("3. ⚡ Только основная отправка (без теста)")
        print("4. 📊 Только анализ (без отправки)")

        while True:
            choice = input("\n➡️ Выберите режим (1-4): ").strip()

            if choice == "1":
                mode = "test-only"
                break
            elif choice == "2":
                mode = "full-with-test"
                break
            elif choice == "3":
                mode = "full-only"
                break
            elif choice == "4":
                mode = "analysis-only"
                break
            else:
                print("❌ Некорректный выбор. Введите число от 1 до 4.")
                continue

        # Выбор мульти-аккаунта для режимов отправки
        use_multi_account = True
        if mode != "analysis-only":
            print("\n👥 НАСТРОЙКИ АККАУНТОВ")
            print("-" * 40)
            print(
                "1. 🔄 Мульти-аккаунт (messenger1 + messenger2, рекомендуется)"
            )
            print("2. 👤 Один аккаунт (только messenger1)")

            while True:
                acc_choice = input("\n➡️ Выберите (1-2): ").strip()
                if acc_choice == "1":
                    use_multi_account = True
                    print(
                        "✅ Будет использоваться мульти-аккаунтовая отправка"
                    )
                    break
                elif acc_choice == "2":
                    use_multi_account = False
                    print("✅ Будет использоваться один аккаунт")
                    break
                else:
                    print("❌ Некорректный выбор. Введите 1 или 2.")

        return mode, use_multi_account

    def get_test_limit(self) -> int:
        """Получает количество тестовых контактов"""
        while True:
            try:
                limit = input(
                    "\n➡️ Количество тестовых контактов (3-20, по умолчанию 3): "
                ).strip()
                if not limit:
                    return 3

                limit = int(limit)
                if 1 <= limit <= 20:
                    return limit
                else:
                    print("❌ Введите число от 1 до 20")
            except ValueError:
                print("❌ Введите корректное число")

    def run_analysis_only(self):
        """Запускает только анализ"""
        print("\n📊 ЗАПУСК АНАЛИЗА")
        print("-" * 40)

        print("Генерируем подробный отчет...")
        self.manager.generate_campaign_report(detailed=True)

        # Предложение экспорта
        if self.ask_confirmation(
            "Экспортировать целевую аудитории в CSV файл?"
        ):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = (
                f"restricted/data/target_audience_analysis_{timestamp}.csv"
            )
            result = self.manager.export_target_audience(output_file)
            print(f"📁 {result}")

    def run_campaign(
        self, mode: str, test_limit: int = 3, use_multi_account: bool = True
    ):
        """Запускает кампанию в выбранном режиме"""
        print(f"\n🚀 ЗАПУСК КАМПАНИИ: {mode.upper()}")
        print("-" * 40)

        # Подготавливаем команду
        cmd = ["python", str(self.current_dir / "first_wave_campaign.py")]

        if mode == "test-only":
            cmd.extend(["--test-only", "--test-limit", str(test_limit)])
        elif mode == "full-only":
            cmd.append("--full-only")
        elif mode == "full-with-test":
            cmd.extend(["--test-limit", str(test_limit)])

        # Добавляем опцию мульти-аккаунта
        if not use_multi_account:
            cmd.append("--single-account")

        print(f"Выполняется команда: {' '.join(cmd)}")
        if use_multi_account:
            print("👥 Режим: Мульти-аккаунт (messenger1 + messenger2)")
        else:
            print("👤 Режим: Один аккаунт (messenger1)")

        print("\n" + "=" * 80)

        try:
            # Запускаем кампанию
            process = subprocess.run(cmd, cwd=self.current_dir.parent)

            if process.returncode == 0:
                print("\n✅ Кампания завершена успешно!")
            else:
                print(
                    f"\n❌ Кампания завершена с ошибкой (код: {process.returncode})"
                )

        except KeyboardInterrupt:
            print("\n⏹️ Кампания остановлена пользователем")
        except Exception as e:
            print(f"\n❌ Ошибка запуска кампании: {e}")

    def show_logs_info(self):
        """Показывает информацию о логах"""
        print("\n📋 ИНФОРМАЦИЯ О ЛОГАХ")
        print("-" * 40)

        log_files = [
            ("first_wave_campaign.log", "Детальный лог выполнения"),
            ("first_wave_campaign_log.json", "Структурированный лог кампаний"),
            ("first_wave_results.csv", "Результаты отправки сообщений"),
        ]

        data_dir = Path("restricted/data")

        for filename, description in log_files:
            file_path = data_dir / filename
            if file_path.exists():
                size = file_path.stat().st_size
                modified = datetime.fromtimestamp(file_path.stat().st_mtime)
                print(f"✅ {filename}")
                print(f"   {description}")
                print(
                    f"   Размер: {size:,} байт, изменен: {modified.strftime('%d.%m.%Y %H:%M')}"
                )
            else:
                print(f"❌ {filename} - файл не найден")
            print()

        print("💡 Для просмотра логов в реальном времени:")
        print("   tail -f restricted/data/first_wave_campaign.log")

    def show_post_campaign_options(self):
        """Показывает опции после кампании"""
        print("\n🔧 ДОПОЛНИТЕЛЬНЫЕ ОПЦИИ")
        print("-" * 40)
        print("1. 📊 Показать отчет по результатам")
        print("2. 📋 Показать информацию о логах")
        print("3. 🧹 Очистить дубликаты в CSV")
        print("4. 📁 Экспортировать оставшуюся целевую аудиторию")
        print("5. ↩️  Вернуться в главное меню")
        print("6. 🚪 Выход")

        while True:
            choice = input("\n➡️ Выберите опцию (1-6): ").strip()

            if choice == "1":
                self.manager.generate_campaign_report(detailed=True)
                break
            elif choice == "2":
                self.show_logs_info()
                break
            elif choice == "3":
                result = self.manager.cleanup_duplicates(dry_run=True)
                print(result)
                if "найдено" in result.lower() and self.ask_confirmation(
                    "Выполнить очистку?"
                ):
                    result = self.manager.cleanup_duplicates(dry_run=False)
                    print(result)
                break
            elif choice == "4":
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = (
                    f"restricted/data/remaining_audience_{timestamp}.csv"
                )
                result = self.manager.export_target_audience(output_file)
                print(f"📁 {result}")
                break
            elif choice == "5":
                return "menu"
            elif choice == "6":
                return "exit"
            else:
                print("❌ Некорректный выбор. Введите число от 1 до 6.")

        return "menu"

    def run(self):
        """Главный цикл интерактивного меню"""
        while True:
            self.print_header()

            # Проверяем готовность системы
            if not self.check_prerequisites():
                print("\n❌ Система не готова к запуску кампании!")
                print("Устраните указанные проблемы и запустите скрипт снова.")
                if self.ask_confirmation("Показать детальную диагностику?"):
                    subprocess.run(
                        [
                            "python",
                            str(self.current_dir / "campaign_manager.py"),
                            "check",
                        ]
                    )
                break

            # Показываем превью целевой аудитории
            if not self.show_target_audience_preview():
                break

            # Выбираем режим кампании
            mode, use_multi_account = self.select_campaign_mode()

            if mode == "analysis-only":
                self.run_analysis_only()
            else:
                # Настройки для тестирования
                test_limit = 3
                if mode in ["test-only", "full-with-test"]:
                    test_limit = self.get_test_limit()

                # Финальное подтверждение
                multi_info = (
                    "с мульти-аккаунтом"
                    if use_multi_account
                    else "с одним аккаунтом"
                )

                if mode == "test-only":
                    message = f"Запустить тестовую отправку на {test_limit} контактов {multi_info}?"
                elif mode == "full-only":
                    message = f"Запустить полную кампанию БЕЗ тестирования {multi_info}?"
                else:
                    message = f"Запустить кампанию (тест {test_limit} + полная отправка) {multi_info}?"

                if not self.ask_confirmation(message):
                    print("❌ Запуск отменен")
                    continue

                # Запускаем кампанию
                self.run_campaign(mode, test_limit, use_multi_account)

            # Послекампанейные опции
            action = self.show_post_campaign_options()
            if action == "exit":
                break
            # Если action == "menu", цикл продолжается

        print("\n👋 До свидания!")


def main():
    """Главная функция"""
    try:
        launcher = InteractiveCampaignLauncher()
        launcher.run()
    except KeyboardInterrupt:
        print("\n\n⏹️ Программа остановлена пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
