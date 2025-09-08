#!/usr/bin/env python3
"""
Создать пустой CSV файл ежедневной статистики с правильными заголовками
"""

import pandas as pd
import os


def create_empty_csv():
    """Создать пустой CSV файл с правильной структурой колонок"""

    # Ensure data directory exists
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)

    csv_file = os.path.join(data_dir, "daily_statistics.csv")

    # Define the columns in Russian
    columns = [
        "Дата",  # Дата в формате YYYY-MM-DD
        "Новых контактов",  # Всего контактов собрано за день
        "Провалидированых",  # Контактов прошедших фильтрацию
        "Отправлено Сообщений",  # Сообщений отправлено за день
        "Ответили",  # Сообщений с полученными ответами
        "Даниил",  # Сообщений отправлено Данилом
        "Ярослав",  # Сообщений отправлено Ярославом
        "Количество follow_up",  # Количество follow-up сообщений
        "Дата последнего follow_up",  # Дата последнего follow-up
        "% Валидных",  # Процент валидных контактов (валидные/собранные)
        "% Ответивших",  # Процент ответивших (ответы/отправлено)
        "% Даниил",  # Процент сообщений Данила
        "% Ярослав",  # Процент сообщений Ярослава
    ]

    # Create empty DataFrame with proper columns
    df = pd.DataFrame(columns=columns)

    # Save to CSV
    df.to_csv(csv_file, index=False)

    print("🎉 CSV файл создан успешно!")
    print(f"📄 Расположение файла: {csv_file}")
    print(f"📋 Созданные колонки:")
    for i, col in enumerate(columns, 1):
        print(f"   {i}. {col}")

    print(f"\n💡 Теперь вы можете:")
    print(f"   • Запустить 'python analytics.py' для заполнения данными")
    print(
        f"   • Запустить 'python add_daily_stats.py' для добавления записей вручную"
    )
    print(
        f"   • Запустить 'python view_stats.py' для просмотра текущих данных"
    )

    # Show example of how to add data
    print(f"\n📝 Пример добавления данных:")
    print(f"   python add_daily_stats.py 2025-09-06 250 80 60 8")

    return csv_file


def main():
    """Главная функция"""
    try:
        # Change to script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)

        create_empty_csv()
        return 0

    except Exception as e:
        print(f"❌ Ошибка создания CSV файла: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
