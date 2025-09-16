"""
Data processing functionality for CSV handling, user data extraction, and filtering
"""

import os
import csv
import re
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional, Any

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class DataProcessor:
    """Handles CSV data processing, user extraction, and data manipulation"""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def extract_user_data_from_csv(
        self,
        csv_file: str,
        apply_filters: bool = True,
        enable_position_filter: bool = True,
    ) -> List[Dict[str, str]]:
        """Extracts user ID and names from CSV file with optional filtering"""
        user_data = []

        if not os.path.exists(csv_file):
            print(f"❌ Файл {csv_file} не знайдено")
            return user_data

        try:
            if PANDAS_AVAILABLE:
                # Read CSV file with more tolerant settings
                try:
                    df = pd.read_csv(csv_file, encoding="utf-8")
                    print(f"📊 Загальна кількість записів: {len(df)}")
                except pd.errors.ParserError as e:
                    print(f"⚠️ Помилка парсингу CSV (спробуємо виправити): {e}")
                    # Try with other parameters
                    try:
                        df = pd.read_csv(
                            csv_file,
                            encoding="utf-8",
                            quoting=1,
                            skipinitialspace=True,
                        )
                        print(
                            f"📊 Загальна кількість записів (після виправлення): {len(df)}"
                        )
                    except Exception as e2:
                        print(f"❌ Критична помилка парсингу CSV: {e2}")
                        print("💡 Спробуємо базову обробку без pandas...")
                        raise ImportError("Fallback to basic CSV processing")
                except UnicodeDecodeError:
                    print(
                        "⚠️ Помилка кодування, спробуємо з іншим кодуванням..."
                    )
                    try:
                        df = pd.read_csv(csv_file, encoding="latin-1")
                        print(
                            f"📊 Загальна кількість записів (latin-1): {len(df)}"
                        )
                    except Exception as e3:
                        print(f"❌ Помилка з усіма кодуваннями: {e3}")
                        raise ImportError("Fallback to basic CSV processing")

                if apply_filters:
                    df = self._apply_pandas_filters(df, enable_position_filter)

                # Convert to user list
                for _, row in df.iterrows():
                    user_info = self._extract_user_from_row(row)
                    if user_info:
                        user_data.append(user_info)
            else:
                raise ImportError("pandas not available")

        except ImportError:
            print(
                "⚠️ pandas не встановлено або помилка парсингу, використовуємо базову обробку..."
            )
            user_data = self._process_csv_basic(csv_file)

        print(f"📋 Знайдено {len(user_data)} користувачів для обробки")
        return user_data

    def _apply_pandas_filters(self, df, enable_position_filter: bool):
        """Apply filters using pandas"""
        print("🔍 Застосовуємо фільтри...")
        original_count = len(df)

        # 1. Filter by empty 'connected' field (if column exists)
        if "connected" in df.columns:
            df = df[df["connected"].isna() | (df["connected"] == "")]
            print(f"   Після фільтру 'connected' (порожнє): {len(df)} записів")
        else:
            print(f"   Колонка 'connected' не знайдена, пропускаємо фільтр")

        # 2. Filter by empty 'Follow-up' field (if column exists)
        if "Follow-up" in df.columns:
            df = df[df["Follow-up"].isna() | (df["Follow-up"] == "")]
            print(f"   Після фільтру 'Follow-up' (порожнє): {len(df)} записів")
        else:
            print(f"   Колонка 'Follow-up' не знайдена, пропускаємо фільтр")

        # 3. Filter by 'valid' field - exclude records with valid="false"
        if "valid" in df.columns:
            before_valid_filter = len(df)
            df = df[df["valid"] != "false"]
            excluded_by_valid = before_valid_filter - len(df)
            print(
                f"   Після фільтру 'valid' (виключено invalid): {len(df)} записів (-{excluded_by_valid} invalid)"
            )
        else:
            print(f"   Колонка 'valid' не знайдена, пропускаємо фільтр")

        # 4. Filter by gaming_vertical (without "land")
        if "gaming_vertical" in df.columns:
            df = df[
                ~df["gaming_vertical"].str.contains(
                    "land", case=False, na=False
                )
            ]
            print(
                f"   Після фільтру gaming_vertical (без 'land'): {len(df)} записів"
            )

        # 5. Filter by position (contains keywords) - only if enabled
        if enable_position_filter:
            df = self._apply_position_filter(df)
        else:
            print("   Фільтр за позиціями вимкнено - включені всі позиції")

        print(f"📊 Відфільтровано: {original_count} → {len(df)} записів")
        return df

    def _apply_position_filter(self, df):
        """Apply position-based filtering"""
        position_keywords = [
            "chief executive officer",
            "ceo",
            "chief operating officer",
            "coo",
            "chief financial officer",
            "cfo",
            "chief payments officer",
            "cpo",
            "payments",
            "psp",
            "operations",
            "business development",
            "partnerships",
            "relationship",
            "country manager",
        ]

        if "position" in df.columns:
            # Convert positions to lowercase for comparison
            df["position_lower"] = df["position"].str.lower().fillna("")

            # Create mask for positions containing keywords
            position_mask = df["position_lower"].str.contains(
                "|".join(position_keywords), case=False, na=False
            )

            # Exclude "coordinator" for COO
            coordinator_mask = df["position_lower"].str.contains(
                "coordinator", case=False, na=False
            )
            coo_mask = df["position_lower"].str.contains(
                "coo", case=False, na=False
            )

            # Apply filter: include positions with keywords, but exclude coordinator with COO
            df = df[position_mask & ~(coo_mask & coordinator_mask)]

            # Remove temporary column
            df = df.drop("position_lower", axis=1)

            print(
                f"   Після фільтру позиції (ключові слова, виключаючи COO+coordinator): {len(df)} записів"
            )

        return df

    def _extract_user_from_row(self, row) -> Optional[Dict[str, str]]:
        """Extract user information from a pandas row"""
        source_url = row.get("source_url", "")
        full_name = row.get("full_name", "")
        company_name = row.get("company_name", "")

        if source_url and full_name:
            # Extract user ID from URL
            match = re.search(r"/attendees/([^/?]+)", source_url)
            if match:
                user_id = match.group(1)

                # Extract first name
                first_name = (
                    full_name.split()[0] if full_name.split() else "there"
                )

                return {
                    "user_id": user_id,
                    "first_name": first_name,
                    "full_name": full_name,
                    "company_name": company_name,
                }
        return None

    def _process_csv_basic(self, csv_file: str) -> List[Dict[str, str]]:
        """Process CSV using basic file operations when pandas isn't available"""
        user_data = []

        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                # First read headers
                first_line = f.readline().strip()
                headers = first_line.split(",")

                # Check if required columns exist
                if "source_url" not in headers or "full_name" not in headers:
                    print(
                        "❌ Не знайдено обов'язкові колонки 'source_url' або 'full_name'"
                    )
                    return user_data

                source_url_idx = headers.index("source_url")
                full_name_idx = headers.index("full_name")
                company_name_idx = (
                    headers.index("company_name")
                    if "company_name" in headers
                    else -1
                )

                line_num = 1
                for line in f:
                    line_num += 1
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        # Simple split by comma (may not work perfectly, but better than nothing)
                        fields = line.split(",")

                        # Check if enough fields
                        max_idx = max(source_url_idx, full_name_idx)
                        if company_name_idx > -1:
                            max_idx = max(max_idx, company_name_idx)

                        if len(fields) > max_idx:
                            source_url = (
                                fields[source_url_idx].strip().strip('"')
                            )
                            full_name = (
                                fields[full_name_idx].strip().strip('"')
                            )
                            company_name = (
                                fields[company_name_idx].strip().strip('"')
                                if company_name_idx > -1
                                and len(fields) > company_name_idx
                                else ""
                            )

                            user_info = self._extract_user_from_data(
                                source_url, full_name, company_name
                            )
                            if user_info:
                                user_data.append(user_info)
                    except Exception as line_error:
                        print(
                            f"⚠️ Пропускаємо пошкоджений рядок {line_num}: {str(line_error)[:50]}..."
                        )
                        continue

        except Exception as file_error:
            print(f"❌ Помилка читання файлу: {file_error}")
            user_data = self._process_csv_fallback(csv_file)

        return user_data

    def _extract_user_from_data(
        self, source_url: str, full_name: str, company_name: str = ""
    ) -> Optional[Dict[str, str]]:
        """Extract user information from raw data"""
        if source_url and full_name:
            # Extract user ID from URL
            match = re.search(r"/attendees/([^/?]+)", source_url)
            if match:
                user_id = match.group(1)

                # Extract first name
                first_name = (
                    full_name.split()[0] if full_name.split() else "there"
                )

                return {
                    "user_id": user_id,
                    "first_name": first_name,
                    "full_name": full_name,
                    "company_name": company_name,
                }
        return None

    def _process_csv_fallback(self, csv_file: str) -> List[Dict[str, str]]:
        """Fallback CSV processing with different encoding"""
        user_data = []
        try:
            # Try with different encoding
            with open(csv_file, "r", encoding="latin-1") as f:
                reader = csv.DictReader(f)
                for row_num, row in enumerate(reader, 2):
                    try:
                        source_url = row.get("source_url", "")
                        full_name = row.get("full_name", "")
                        company_name = row.get("company_name", "")

                        user_info = self._extract_user_from_data(
                            source_url, full_name, company_name
                        )
                        if user_info:
                            user_data.append(user_info)
                    except Exception as row_error:
                        print(
                            f"⚠️ Пропускаємо пошкоджений рядок {row_num}: {str(row_error)[:50]}..."
                        )
                        continue
        except Exception as final_error:
            print(f"❌ Критична помилка обробки файлу: {final_error}")

        return user_data

    def extract_user_id_from_url(self, source_url: str) -> str:
        """Extracts user_id from source_url"""
        if not source_url:
            return ""
        try:
            match = re.search(r"/attendees/([^/?]+)", source_url)
            return match.group(1) if match else ""
        except:
            return ""

    def parse_date_flexible(self, date_str, current_date) -> datetime:
        """Flexible date parsing in various formats"""
        # Check for NaN values without requiring pandas in scope
        if (
            date_str is None
            or str(date_str).lower() in ["nan", "", "none"]
            or (
                hasattr(date_str, "__class__")
                and "float" in str(date_str.__class__)
                and str(date_str) == "nan"
            )
        ):
            return current_date

        date_str = str(date_str).strip()
        if not date_str:
            return current_date

        kyiv_tz = ZoneInfo("Europe/Kiev")

        try:
            # Try multiple date formats
            for fmt in [
                "%Y-%m-%d",
                "%d.%m.%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%d.%m.%Y %H:%M:%S",
            ]:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    # If no timezone, assume Kyiv
                    if parsed_date.tzinfo is None:
                        parsed_date = parsed_date.replace(tzinfo=kyiv_tz)
                    return parsed_date
                except ValueError:
                    continue

        except Exception:
            pass

        return None

    def fix_malformed_csv(self, csv_file: str, backup: bool = True) -> bool:
        """Fixes malformed CSV file"""
        if backup:
            backup_file = f"{csv_file}.backup"
            try:
                shutil.copy2(csv_file, backup_file)
                print(f"📁 Створено backup: {backup_file}")
            except Exception as e:
                print(f"⚠️ Не вдалося створити backup: {e}")

        try:
            fixed_rows = []

            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

            if not lines:
                print("❌ Файл порожній")
                return False

            # First line - headers
            header_line = lines[0].strip()
            headers = [h.strip().strip('"') for h in header_line.split(",")]
            expected_fields = len(headers)

            print(f"📊 Очікується {expected_fields} полів на рядок")
            print(f"📋 Заголовки: {', '.join(headers[:5])}...")

            fixed_rows.append(header_line)

            for line_num, line in enumerate(lines[1:], 2):
                line = line.strip()
                if not line:
                    continue

                # Count fields
                fields = line.split(",")

                if len(fields) == expected_fields:
                    # Line is correct
                    fixed_rows.append(line)
                elif len(fields) > expected_fields:
                    # Too many fields - possibly unprotected commas in data
                    print(
                        f"⚠️ Рядок {line_num}: {len(fields)} полів замість {expected_fields}"
                    )

                    # Try to keep only first required fields
                    fixed_line = ",".join(fields[:expected_fields])
                    fixed_rows.append(fixed_line)
                    print(f"✅ Виправлено рядок {line_num}")
                else:
                    # Too few fields - skip
                    print(
                        f"❌ Пропускаємо рядок {line_num}: тільки {len(fields)} полів"
                    )
                    continue

            # Write fixed CSV
            with open(csv_file, "w", encoding="utf-8") as f:
                f.write("\n".join(fixed_rows))

            print(f"✅ Виправлено CSV файл: {len(fixed_rows)} рядків")
            return True

        except Exception as e:
            print(f"❌ Помилка виправлення CSV: {e}")
            return False

    def update_csv_with_messaging_status(
        self, csv_file: str, user_id: str, full_name: str, chat_id: str = None
    ):
        """Updates CSV file with messaging information"""
        try:
            if PANDAS_AVAILABLE:
                self._update_csv_pandas(
                    csv_file, user_id, full_name, "connected", "Sent", chat_id
                )
            else:
                print("⚠️ pandas недоступний для оновлення CSV")
        except ImportError:
            print("⚠️ pandas недоступний для оновлення CSV")
        except Exception as e:
            print(f"❌ Помилка оновлення CSV: {e}")

    def update_csv_excluded_company(
        self, csv_file: str, user_id: str, full_name: str, company_name: str
    ):
        """Updates CSV file for excluded companies, setting valid=false"""
        try:
            if PANDAS_AVAILABLE:
                self._update_csv_pandas(
                    csv_file, user_id, full_name, "valid", "false"
                )
            else:
                print("⚠️ pandas недоступний для оновлення CSV")
        except ImportError:
            print("⚠️ pandas недоступний для оновлення CSV")
        except Exception as e:
            print(f"❌ Помилка оновлення CSV для виключеної компанії: {e}")

    def update_csv_response_status(
        self,
        csv_file: str,
        user_id: str,
        has_response: bool,
        participant_name: str = None,
        chat_id: str = None,
    ):
        """Updates response status in CSV file by user_id, creates new row if needed"""
        try:
            if PANDAS_AVAILABLE:
                status_value = "Yes" if has_response else "No"
                self._update_csv_pandas(
                    csv_file,
                    user_id,
                    participant_name,
                    "Responded",
                    status_value,
                    chat_id,
                )
            else:
                print("⚠️ pandas недоступний для оновлення CSV")
        except ImportError:
            print("⚠️ pandas недоступний для оновлення CSV")
        except Exception as e:
            print(f"❌ Помилка оновлення статусу відповіді: {e}")

    def update_csv_with_chat_id(
        self,
        csv_file: str,
        user_id: str,
        chat_id: str,
        participant_name: str = None,
    ):
        """Updates CSV file with chat_id for specific user, creates new row if needed"""
        try:
            if PANDAS_AVAILABLE:
                self._update_csv_pandas(
                    csv_file, user_id, participant_name, "chat_id", chat_id
                )
            else:
                print("⚠️ pandas недоступний для оновлення CSV")
        except ImportError:
            print("⚠️ pandas недоступний для оновлення CSV")
        except Exception as e:
            print(f"❌ Помилка оновлення chat_id: {e}")

    def _update_csv_pandas(
        self,
        csv_file: str,
        user_id: str,
        full_name: str,
        column: str,
        value: str,
        chat_id: str = None,
    ):
        """Helper method to update CSV using pandas"""
        df = pd.read_csv(csv_file)

        # Try to find existing record
        user_mask = df["source_url"].str.contains(
            f"/attendees/{user_id}", na=False
        )

        if user_mask.any():
            # Update existing record
            df.loc[user_mask, column] = value
            if chat_id and "chat_id" in df.columns:
                df.loc[user_mask, "chat_id"] = chat_id
        else:
            # Create new record if needed
            if full_name:
                new_row = {
                    "source_url": f"https://sbcconnect.com/attendees/{user_id}",
                    "full_name": full_name,
                    column: value,
                }
                if chat_id:
                    new_row["chat_id"] = chat_id

                # Convert to DataFrame and append
                new_df = pd.DataFrame([new_row])
                df = pd.concat([df, new_df], ignore_index=True)

        df.to_csv(csv_file, index=False)

    def _update_csv_followup_pandas(
        self, csv_file: str, chat_id: str, followup_type: str
    ):
        """Helper method to update follow-up status using pandas"""
        df = pd.read_csv(csv_file)

        # Find records with matching chat_id
        if "chat_id" in df.columns:
            chat_mask = df["chat_id"] == chat_id
            if chat_mask.any():
                df.loc[chat_mask, "Follow-up"] = followup_type
                df.to_csv(csv_file, index=False)

    def create_csv_row_for_participant(
        self, csv_file: str, user_id: str, participant_name: str, chat_id: str
    ) -> bool:
        """Creates new row in CSV for participant who wasn't in initial database"""
        try:
            if PANDAS_AVAILABLE:
                df = pd.read_csv(csv_file)

                new_row = {
                    "source_url": f"https://sbcconnect.com/attendees/{user_id}",
                    "full_name": participant_name,
                    "chat_id": chat_id,
                    "connected": "Sent",
                    "author": "System-Generated",
                }

                # Add row
                new_df = pd.DataFrame([new_row])
                df = pd.concat([df, new_df], ignore_index=True)
                df.to_csv(csv_file, index=False)

                print(f"✅ Створено новий рядок для {participant_name}")
                return True
            else:
                print("⚠️ pandas недоступний для створення нового рядка")
                return False
        except Exception as e:
            print(f"❌ Помилка створення нового рядка: {e}")
            return False

    def _check_followup_in_csv(
        self, csv_file: str, chat_id: str, followup_type: str
    ) -> bool:
        """Checks if followup has been sent according to CSV"""
        try:
            with open(csv_file, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if row.get("Chat ID") == chat_id:
                        followup_status = row.get(
                            f"{followup_type.title()} Follow-up", ""
                        ).strip()
                        return followup_status.lower() in [
                            "true",
                            "yes",
                            "1",
                            "sent",
                        ]
            return False
        except Exception as e:
            print(f"❌ Помилка перевірки follow-up в CSV: {e}")
            return False

    def update_csv_followup_status(
        self,
        csv_file: str,
        chat_id: str,
        followup_type: str,
        chat_data: dict = None,
    ):
        """Оновлює статус Follow-up в CSV файлі після відправки з підтримкою conference_active та створення нових записів"""
        try:
            import pandas as pd
            from zoneinfo import ZoneInfo
            from datetime import datetime

            df = pd.read_csv(csv_file)

            # Спочатку шукаємо запис за chat_id
            mask = df["chat_id"] == chat_id
            found_row = False

            if mask.any():
                found_row = True
                print(f"       📋 Знайдено запис за chat_id: {chat_id}")
            else:
                print(
                    f"       🔍 Запис з chat_id {chat_id} не знайдено, шукаємо за user_id..."
                )

                # Якщо chat_data надано, спробуємо знайти за user_id з учасників чату
                if chat_data:
                    # Note: We need to get current_user_id from the base_scraper
                    # This might need to be passed as a parameter in the future
                    current_user_id = getattr(self, "_current_user_id", None)
                    if not current_user_id:
                        print(f"       ⚠️ Не вдалося отримати current_user_id")
                        return False

                    participant_id = None
                    participant_name = "Unknown"

                    # Знаходимо ID співрозмовника (не нас)
                    if chat_data.get("participants"):
                        for participant in chat_data["participants"]:
                            if participant.get("userId") != current_user_id:
                                participant_id = participant.get("userId")
                                first_name = participant.get("firstName", "")
                                last_name = participant.get("lastName", "")
                                participant_name = (
                                    f"{first_name} {last_name}".strip()
                                    or "Unknown"
                                )
                                break

                    if participant_id:
                        # Шукаємо за source_url, що містить цей user_id
                        source_mask = df["source_url"].str.contains(
                            participant_id, na=False
                        )
                        if source_mask.any():
                            mask = source_mask
                            found_row = True
                            print(
                                f"       ✅ Знайдено запис за user_id: {participant_id}"
                            )
                        else:
                            print(
                                f"       ➕ Користувач {participant_name} ({participant_id}) не знайдений у CSV, створюємо новий запис..."
                            )

                            # Створюємо новий рядок для цього користувача
                            new_row = {
                                "full_name": participant_name,
                                "company_name": "Unknown",
                                "position": "Unknown",
                                "source_url": f"https://sbcconnect.com/event/sbc-summit-2025/attendees/{participant_id}",
                                "connected": "",
                                "Follow-up": "true",
                                "valid": "Valid",
                                "author": "System",  # Default author since we don't have access to accounts here
                                "chat_id": chat_id,
                            }

                            # Додаємо новий рядок до DataFrame
                            new_df = pd.DataFrame([new_row])
                            df = pd.concat([df, new_df], ignore_index=True)

                            # Оновлюємо mask для нового рядка
                            mask = df.index == (len(df) - 1)
                            found_row = True
                            print(
                                f"       ✅ Створено новий запис для {participant_name}"
                            )

            if found_row:
                # Handle different followup types appropriately
                if followup_type == "conference_active":
                    # For conference active messages, use dedicated column
                    if "Conference Active Status" not in df.columns:
                        df["Conference Active Status"] = ""
                    df.loc[mask, "Conference Active Status"] = "sent"

                    # Also set the general Follow-up status
                    df.loc[mask, "Follow-up"] = "true"

                    # Update chat_id if it was missing
                    if "chat_id" not in df.columns:
                        df["chat_id"] = ""
                    df.loc[mask, "chat_id"] = chat_id

                    # Update Follow-up type column to include conference_active
                    if "Follow-up type" not in df.columns:
                        df["Follow-up type"] = ""
                    current_type = df.loc[mask, "Follow-up type"].iloc[0]
                    if pd.isna(current_type) or str(current_type) == "":
                        df.loc[mask, "Follow-up type"] = "conference_active"
                    elif "conference_active" not in str(current_type):
                        df.loc[mask, "Follow-up type"] = (
                            f"{current_type},conference_active"
                        )
                else:
                    # For other followup types, use the standard logic
                    df.loc[mask, "Follow-up"] = "true"

                    # Update chat_id if it was missing
                    if "chat_id" not in df.columns:
                        df["chat_id"] = ""
                    df.loc[mask, "chat_id"] = chat_id

                    # Оновлюємо Follow-up type колонку
                    if "Follow-up type" not in df.columns:
                        df["Follow-up type"] = ""
                    df.loc[mask, "Follow-up type"] = (
                        f"follow-up_{followup_type}"
                    )

                # ВАЖЛИВО: Записуємо дату відправки follow-up
                kyiv_tz = ZoneInfo("Europe/Kiev")
                current_date = datetime.now(kyiv_tz)
                formatted_date = current_date.strftime("%d.%m.%Y")

                # Додаємо колонку follow_up_date якщо її немає
                if "follow_up_date" not in df.columns:
                    df["follow_up_date"] = ""

                df.loc[mask, "follow_up_date"] = formatted_date

                # Зберігаємо оновлений файл
                df.to_csv(csv_file, index=False, encoding="utf-8")

                print(
                    f"       📝 Follow-up статус оновлено: {followup_type}, дата: {formatted_date}"
                )
                return True
            else:
                print(
                    f"       ❌ Не вдалося знайти або створити запис для chat_id {chat_id}"
                )
                return False

        except ImportError:
            print(
                f"       ⚠️ pandas не встановлено, Follow-up статус не оновлено"
            )
            return False
        except Exception as e:
            print(f"       ❌ Помилка оновлення Follow-up статусу: {e}")
            import traceback

            traceback.print_exc()
            return False

    def update_csv_response_status_by_chat_id(
        self,
        csv_file: str,
        chat_id: str,
        has_response: bool,
        participant_name: str = None,
        participant_id: str = None,
    ) -> bool:
        """Updates CSV response status by Chat ID"""
        try:
            # Read current CSV
            rows = []
            updated = False

            with open(csv_file, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames

                # Ensure required columns exist
                required_columns = ["Response Status", "Response Date"]
                for col in required_columns:
                    if col not in fieldnames:
                        fieldnames = list(fieldnames) + [col]

                for row in reader:
                    if row.get("Chat ID") == chat_id:
                        row["Response Status"] = (
                            "Response" if has_response else "No Response"
                        )
                        if has_response:
                            from datetime import datetime

                            row["Response Date"] = datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        if participant_name:
                            if "Participant Name" not in fieldnames:
                                fieldnames = list(fieldnames) + [
                                    "Participant Name"
                                ]
                            row["Participant Name"] = participant_name
                        updated = True
                    rows.append(row)

            if updated:
                # Write updated CSV
                with open(csv_file, "w", encoding="utf-8", newline="") as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

                print(f"✅ Оновлено статус відповіді для Chat ID: {chat_id}")

            return updated
        except Exception as e:
            print(f"❌ Помилка оновлення статусу відповіді: {e}")
            return False

    def _get_relevant_chat_ids_from_csv(self, csv_file: str) -> set:
        """Gets chat IDs that need response checking (Sent/Empty/True status)"""
        relevant_chat_ids = set()
        try:
            with open(csv_file, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Check if message was sent and no response yet
                    sent_status = row.get("Sent", "").strip().lower()
                    response_status = (
                        row.get("Response Status", "").strip().lower()
                    )
                    chat_id = row.get("Chat ID", "").strip()

                    if chat_id and sent_status in [
                        "true",
                        "yes",
                        "1",
                        "sent",
                        "",
                    ]:
                        if not response_status or response_status in [
                            "",
                            "no response",
                            "false",
                        ]:
                            relevant_chat_ids.add(chat_id)

        except Exception as e:
            print(f"❌ Помилка читання CSV для фільтрації: {e}")

        return relevant_chat_ids

    def get_followup_candidates_from_csv(
        self, csv_file: str, followup_type: str, check_columns: list = None
    ) -> list:
        """Gets candidates for follow-up campaigns from CSV"""
        candidates = []

        if check_columns is None:
            check_columns = ["Sent"]

        try:
            with open(csv_file, "r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file)

                for row in reader:
                    chat_id = row.get("Chat ID", "").strip()
                    if not chat_id:
                        continue

                    # Check if eligible for this followup type
                    eligible = True

                    # Check if already sent this followup
                    followup_column = f"{followup_type.title()} Follow-up"
                    if followup_column in row:
                        followup_status = (
                            row.get(followup_column, "").strip().lower()
                        )
                        if followup_status in ["true", "yes", "1", "sent"]:
                            eligible = False

                    # Check required conditions
                    if eligible:
                        for col in check_columns:
                            if col in row:
                                value = row.get(col, "").strip().lower()
                                if value not in [
                                    "true",
                                    "yes",
                                    "1",
                                    "sent",
                                    "",
                                ]:
                                    eligible = False
                                    break

                    if eligible:
                        candidate = {
                            "chat_id": chat_id,
                            "first_name": row.get("First Name", ""),
                            "last_name": row.get("Last Name", ""),
                            "email": row.get("Email", ""),
                            "company": row.get("Company", ""),
                            "row_data": row,
                        }
                        candidates.append(candidate)

        except Exception as e:
            print(f"❌ Помилка отримання кандидатів для follow-up: {e}")

        return candidates
