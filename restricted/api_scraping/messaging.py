"""
Messaging functionality for chat management, message sending, and follow-up campaigns
"""

import uuid
import time
import json
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Any
import os


class MessagingHandler:
    """Handles all messaging functionality including chats, messages, and follow-up campaigns"""

    def __init__(self, base_scraper, company_filter, data_processor):
        self.base_scraper = base_scraper
        self.company_filter = company_filter
        self.data_processor = data_processor
        self.existing_chats = {}  # Cache of existing chats {user_id: chat_id}

        # Follow-up message templates
        self.follow_up_messages = [
            "Hello {name} !\nI'm thrilled to see you at the SBC Summit in Lisbon this week! Before things get hectic, it's always a pleasure to connect with other iGaming experts.\nI speak on behalf of Flexify Finance, a company that specializes in smooth payments for high-risk industries. Visit us at Stand E613 if you're looking into new payment options or simply want to discuss innovation.\nWhat is your main objective or priority for the expo this year? I'd love to know what you're thinking about!",
            "Hi {name} !\nExcited to connect with fellow SBC Summit attendees! I'm representing Flexify Finance - we provide payment solutions specifically designed for iGaming and high-risk industries.\nWe'll be at Stand E613 during the summit in Lisbon. Would love to learn about your current payment challenges or discuss the latest trends in our industry.\nWhat brings you to SBC Summit this year? Any specific goals or connections you're hoping to make?",
            "Hello {name} !\nLooking forward to the SBC Summit in Lisbon! As someone in the iGaming space, I always enjoy connecting with industry professionals before the event buzz begins.\nI'm with Flexify Finance - we specialize in seamless payment processing for high-risk sectors. Feel free to stop by Stand E613 if you'd like to explore new payment innovations.\nWhat are you most excited about at this year's summit? Any particular sessions or networking goals?",
            "Hi {name}, looks like we'll both be at SBC Lisbon today!\nAlways great to meet fellow iGaming pros before the chaos begins.\nI'm with Flexify Finance, a payments provider for high-risk verticals - you'll find us at Stand E613.\nOut of curiosity, what's your main focus at the expo this year ?",
        ]

        # Second follow-up message that always gets sent after the first one
        self.second_follow_up_message = (
            "Is payments something on your radar to explore ?"
        )

        # Follow-up messages for response tracking
        self.follow_up_templates = {
            "day_3": "Hello, {name}\nJust to follow up, Flexify Finance will be present at SBC Summit Lisbon at Stand E613. With more than 80 local payment options, we're helping iGaming brands expand in high-risk markets while also holding a prize draw.\nWould you have a few minutes during the expo? It would be great to connect.",
            "day_7": "Hello {name}!\nJust wanted to gently follow up and let you know that Flexify Finance will be at SBC Summit Lisbon (Stand E613). We're supporting iGaming brands in high-risk markets with 80+ local payment options - and we'll also have a fun prize draw at our stand.\nIf you have a few minutes during the expo, I'd really enjoy connecting and having a quick chat.",
            "final": "Hi {name}!\nSBC Summit Lisbon starts tomorrow! 🎉\nFlexify Finance will be at Stand E613 with 80+ local payment solutions for high-risk markets. We'd love to meet you in person and discuss how we can help your iGaming business grow.\nDon't miss our prize draw at the stand! Looking forward to seeing you there.",
            "conference_active": {
                "en": "We're already at the conference! We're easy to find. The big all-seeing eye 👁️ will show you the way to the Flexify booth.",
                "ua": "Ми вже на конференції! Нас легко знайти. Наше велике око 👁️ покаже вам шлях до стенду Flexify.",
                "ru": "Мы уже на конференции! Нас легко найти. Наш большой глаз 👁️ покажет вам путь к стенду Flexify.",
            },
        }

        # SBC Summit start date (September 16, 2025) in Kyiv timezone
        kyiv_tz = ZoneInfo("Europe/Kiev")
        self.sbc_start_date = datetime(2025, 9, 16, tzinfo=kyiv_tz)

    def load_chats_list(self, accounts):
        """Loads the list of existing chats"""
        if not isinstance(accounts, dict):
            print(f"❌ Помилка: accounts не є dict, а {type(accounts)}")
            return []

        current_user_id = accounts.get(
            self.base_scraper.current_account, {}
        ).get("user_id")
        if not current_user_id:
            print(
                f"❌ Не знайдено user_id для акаунта {self.base_scraper.current_account}"
            )
            return []

        endpoint = "chat/LoadChatsList?eventPath=sbc-summit-2025"
        print(f"🔍 Завантажуємо список чатів з API...")
        chats_data = self.base_scraper.api_request("GET", endpoint)

        if chats_data and isinstance(chats_data, list):
            # Get current user ID
            # current_user_id = accounts[self.base_scraper.current_account][
            #     "user_id"
            # ]

            # Cache chats for quick access
            for chat in chats_data:
                if not isinstance(chat, dict):
                    continue

                chat_id = chat.get("chatId")
                if not chat_id:
                    continue

                # For single chats (personal chats)
                if chat.get("isSingleChat") and chat.get("singleChatDetails"):
                    single_chat_details = chat.get("singleChatDetails")
                    if isinstance(single_chat_details, dict):
                        user_info = single_chat_details.get("user", {})
                        other_participant_id = user_info.get("id")

                        if (
                            other_participant_id
                            and other_participant_id != current_user_id
                        ):
                            self.existing_chats[other_participant_id] = chat_id

            print(f"📋 Закешовано {len(self.existing_chats)} існуючих чатів")
            return chats_data
        else:
            return []

    def find_chat_with_user(self, target_user_id: str) -> Optional[str]:
        """Finds existing chat with user"""
        return self.existing_chats.get(target_user_id)

    def check_chat_has_messages(self, chat_id: str) -> bool:
        """Checks if chat has messages"""
        endpoint = f"chat/LoadChat?chatId={chat_id}"
        chat_data = self.base_scraper.api_request("GET", endpoint)

        if chat_data and isinstance(chat_data, dict):
            messages = chat_data.get("messages", [])

            if messages:
                print(f"       📝 Знайдено {len(messages)} повідомлень у чаті")
                # Show last message for context
                last_message = messages[-1]
                last_msg_preview = last_message.get("message", "")[:50] + "..."
                print(f"       📄 Останнє повідомлення: '{last_msg_preview}'")
                return True
            else:
                print(f"       📭 Чат порожній (без повідомлень)")
                return False
        else:
            print(f"       ⚠️ Не вдалося завантажити дані чату")
            return False

    def create_chat(self, target_user_id: str, accounts) -> Optional[str]:
        """Creates new chat with user"""
        if not isinstance(accounts, dict):
            print(f"❌ Помилка: accounts не є dict, а {type(accounts)}")
            return None

        current_user_id = accounts.get(
            self.base_scraper.current_account, {}
        ).get("user_id")
        if not current_user_id:
            print(
                f"❌ Не знайдено user_id для акаунта {self.base_scraper.current_account}"
            )
            return None

        chat_id = str(uuid.uuid4())
        endpoint = "chat/createChat"
        data = {
            "eventPath": "sbc-summit-2025",
            "participants": [current_user_id, target_user_id],
            "chatId": chat_id,
        }

        result = self.base_scraper.api_request("POST", endpoint, data)

        if result is True or result is not None:
            self.existing_chats[target_user_id] = chat_id
            return chat_id
        else:
            return None

    def send_message(self, chat_id: str, message: str) -> bool:
        """Sends message to chat"""
        message_id = str(uuid.uuid4())
        # Create timestamp in UTC format with milliseconds
        current_time = (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

        endpoint = "chat/sendMessage"
        data = {
            "chatId": chat_id,
            "messageId": message_id,
            "message": message,
            "createdDate": current_time,
        }

        result = self.base_scraper.api_request("POST", endpoint, data)
        return result is not None

    def send_message_to_user(
        self,
        target_user_id: str,
        message: str,
        accounts,
        full_name: str = None,
        company_name: str = None,
    ) -> str:
        """Complete pipeline for sending message to user with automatic follow-up

        Returns:
        - "success": message successfully sent
        - "already_contacted": chat already contains messages, skipping
        - "failed": sending error
        - "excluded_company": company in exclusion list
        """
        # 0. Check if company is in exclusion list
        if company_name:
            details = self.company_filter.get_exclusion_details(company_name)
            if details["is_excluded"]:
                print(f"       🚫 КОМПАНІЯ ВИКЛЮЧЕНА: '{company_name}'")
                print(
                    f"       📋 Співпадіння з: '{details['matched_company']}' (схожість: {details['similarity_score']:.2f})"
                )
                # Update CSV with valid=false instead of skipping
                if full_name:
                    csv_file = os.path.join(
                        self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
                    )
                    self.data_processor.update_csv_excluded_company(
                        csv_file, target_user_id, full_name, company_name
                    )
                return "excluded_company"

        # 1. Check if there's an existing chat
        chat_id = self.find_chat_with_user(target_user_id)

        if chat_id:
            # 1.1. If chat exists, check if it has messages
            print(
                f"       🔍 Перевіряємо чи є повідомлення в існуючому чаті..."
            )
            if self.check_chat_has_messages(chat_id):
                print(f"       ⏭️ Чат вже містить повідомлення, пропускаємо")
                # Update CSV with "Sent" status since contact was already processed
                if full_name:
                    csv_file = os.path.join(
                        self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
                    )
                    self.data_processor.update_csv_with_messaging_status(
                        csv_file, target_user_id, full_name, chat_id
                    )
                return "already_contacted"
            else:
                print(
                    f"       ✅ Чат порожній, можна відправляти повідомлення"
                )
        else:
            # 2. Create new chat
            print(f"       🆕 Створюємо новий чат...")
            chat_id = self.create_chat(target_user_id, accounts)
            if not chat_id:
                return "failed"

        # 3. Send first message
        if not self.send_message(chat_id, message):
            return "failed"

        # 4. Wait 5 seconds and send second message
        print(f"       ✅ Перше повідомлення відправлено")
        print(f"       ⏱️ Чекаємо 5 секунд перед другим повідомленням...")
        time.sleep(5)

        # 5. Send second message
        if not self.send_message(chat_id, self.second_follow_up_message):
            print(f"       ⚠️ Не вдалося відправити друге повідомлення")
            return "failed"

        print(
            f"       ✅ Друге повідомлення відправлено: '{self.second_follow_up_message}'"
        )

        # 6. Update CSV file about sent messages
        print(f"       📝 Оновлюємо CSV файл...")
        csv_file = os.path.join(
            self.base_scraper.get_data_dir(), "SBC - Attendees.csv"
        )
        if full_name:
            self.data_processor.update_csv_with_messaging_status(
                csv_file, target_user_id, full_name, chat_id
            )
        else:
            print(
                f"       ⚠️ Не вдалося оновити CSV - відсутнє ім'я користувача"
            )

        return "success"

    def load_chat_details(self, chat_id: str) -> Optional[Dict]:
        """Loads detailed chat information"""
        endpoint = f"chat/LoadChat?chatId={chat_id}"
        return self.base_scraper.api_request("GET", endpoint)

    def parse_message_timestamp(
        self, timestamp_str: str
    ) -> Optional[datetime]:
        """Parses message timestamp to datetime object"""
        try:
            # Timestamp format: "2025-09-05T10:30:00.123Z"
            if timestamp_str.endswith("Z"):
                # Remove Z and add UTC timezone
                timestamp_str = timestamp_str[:-1] + "+00:00"

            # Parse the timestamp
            dt = datetime.fromisoformat(timestamp_str)

            # Ensure timezone-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))

            return dt
        except Exception as e:
            print(f"⚠️ Помилка парсингу timestamp {timestamp_str}: {e}")
            return None

    def analyze_chat_for_followup(self, chat_data: Dict, accounts) -> Dict:
        """Analyzes chat to determine follow-up necessity"""
        result = {
            "needs_followup": False,
            "followup_type": None,
            "first_message_date": None,
            "participant_name": None,
            "participant_id": None,
            "days_since_first": 0,
            "has_response": False,
        }

        if not chat_data or not isinstance(chat_data, dict):
            return result

        messages = chat_data.get("messages", [])
        if not messages:
            return result

        # Get current user ID
        current_user_id = accounts[self.base_scraper.current_account][
            "user_id"
        ]

        # Get chat participant information
        if chat_data.get("isSingleChat") and chat_data.get("participants"):
            # Find participant who is not current user
            participants = chat_data.get("participants", [])
            for participant in participants:
                if participant.get("userId") != current_user_id:
                    result["participant_id"] = participant.get("userId")
                    result["participant_name"] = (
                        f"{participant.get('firstName', '')} {participant.get('lastName', '')}".strip()
                    )
                    break

        # Sort messages by time
        sorted_messages = sorted(
            messages, key=lambda x: x.get("createdDate", "")
        )

        # Find first message from us
        first_our_message = None
        for msg in sorted_messages:
            if msg.get("userId") == current_user_id:
                first_our_message = msg
                break

        if not first_our_message:
            return result

        # Parse first message date
        first_message_timestamp = self.parse_message_timestamp(
            first_our_message.get("createdDate", "")
        )
        if not first_message_timestamp:
            return result

        result["first_message_date"] = first_message_timestamp

        # Calculate days since first message
        kyiv_tz = ZoneInfo("Europe/Kiev")

        # Convert to Kyiv time for consistency
        if first_message_timestamp.tzinfo is None:
            # If no timezone info, assume UTC
            first_message_timestamp = first_message_timestamp.replace(
                tzinfo=ZoneInfo("UTC")
            )

        current_time = datetime.now(kyiv_tz)
        first_message_kyiv = first_message_timestamp.astimezone(kyiv_tz)

        days_diff = (current_time.date() - first_message_kyiv.date()).days
        result["days_since_first"] = days_diff

        # Check if there are responses from participant after our first message
        for msg in sorted_messages:
            msg_timestamp = self.parse_message_timestamp(
                msg.get("createdDate", "")
            )
            if msg.get("userId") != current_user_id and msg_timestamp:
                # Convert msg_timestamp to timezone-aware if needed
                if msg_timestamp.tzinfo is None:
                    msg_timestamp = msg_timestamp.replace(
                        tzinfo=ZoneInfo("UTC")
                    )

                # Check if response came after our first message
                if msg_timestamp > first_message_timestamp:
                    result["has_response"] = True
                    break

        # If no response, determine follow-up type
        if not result["has_response"]:
            # Check current time vs conference date
            current_time_kyiv = datetime.now(kyiv_tz)
            if current_time_kyiv.date() >= self.sbc_start_date.date():
                result["followup_type"] = "conference_active"
                result["needs_followup"] = True
            elif days_diff >= 7:
                result["followup_type"] = "final"
                result["needs_followup"] = True
            elif days_diff >= 3:
                result["followup_type"] = "day_7"
                result["needs_followup"] = True
            elif days_diff >= 1:
                result["followup_type"] = "day_3"
                result["needs_followup"] = True

        return result

    def send_followup_message(
        self,
        chat_id: str,
        followup_type: str,
        participant_name: str,
        language: str = "en",
    ) -> bool:
        """Sends follow-up message with language support"""
        if followup_type not in self.follow_up_templates:
            print(f"❌ Невідомий тип follow-up: {followup_type}")
            return False

        # Get first name
        first_name = (
            participant_name.split()[0]
            if participant_name.split()
            else "there"
        )

        # Format message
        template = self.follow_up_templates[followup_type]

        # Handle multi-language templates (like conference_active)
        if isinstance(template, dict):
            # Use requested language or default to English
            message_template = template.get(language, template.get("en", ""))
            if not message_template:
                message_template = list(template.values())[
                    0
                ]  # First available
        else:
            # Regular string template
            message_template = template

        # Format message with name if it contains {name}
        if "{name}" in message_template:
            message = message_template.format(name=first_name)
        else:
            message = message_template

        # Send message
        return self.send_message(chat_id, message)

    def detect_language(self, text: str) -> str:
        """Detects the language of a message using simple keyword matching"""
        if not text:
            return "unknown"

        text_lower = text.lower()

        # English keywords and patterns
        english_indicators = [
            "the",
            "and",
            "you",
            "that",
            "will",
            "with",
            "have",
            "this",
            "for",
            "not",
            "are",
            "but",
            "what",
            "all",
            "were",
            "they",
            "been",
            "said",
            "each",
            "which",
            "their",
            "time",
            "would",
            "about",
            "if",
            "up",
            "out",
            "many",
            "then",
            "them",
            "these",
            "so",
            "some",
            "her",
            "would",
            "make",
            "like",
            "into",
            "him",
            "has",
            "two",
            "more",
            "very",
            "after",
            "words",
            "long",
            "than",
            "first",
            "water",
            "been",
            "call",
            "who",
            "its",
            "now",
            "find",
            "long",
            "down",
            "day",
            "did",
            "get",
            "come",
            "made",
            "may",
            "part",
        ]

        # Ukrainian specific keywords
        ukrainian_indicators = [
            "та",
            "що",
            "не",
            "на",
            "в",
            "я",
            "з",
            "до",
            "від",
            "за",
            "про",
            "під",
            "над",
            "при",
            "або",
            "але",
            "це",
            "як",
            "так",
            "уже",
            "тут",
            "там",
            "коли",
            "де",
            "чому",
            "хто",
            "який",
            "яка",
            "які",
            "для",
            "без",
            "через",
            "після",
            "перед",
            "між",
            "серед",
            "поза",
            "крім",
            "окрім",
            "разом",
            "українською",
            "україна",
            "київ",
            "львів",
            "одеса",
            "харків",
            "дніпро",
        ]

        # Russian specific keywords
        russian_indicators = [
            "и",
            "не",
            "на",
            "в",
            "я",
            "с",
            "до",
            "от",
            "за",
            "про",
            "под",
            "над",
            "при",
            "или",
            "но",
            "это",
            "как",
            "так",
            "уже",
            "тут",
            "там",
            "когда",
            "где",
            "почему",
            "кто",
            "какой",
            "какая",
            "какие",
            "для",
            "без",
            "русским",
            "россия",
            "москва",
            "санкт-петербург",
            "новосибирск",
        ]

        # Count matches
        english_score = sum(
            1 for word in english_indicators if word in text_lower
        )
        ukrainian_score = sum(
            1 for word in ukrainian_indicators if word in text_lower
        )
        russian_score = sum(
            1 for word in russian_indicators if word in text_lower
        )

        # Check for specific Ukrainian characters
        has_ukrainian_chars = bool(re.search(r"[іїєґ]", text_lower))

        # Check for Cyrillic characters
        has_cyrillic = bool(re.search(r"[а-яё]", text_lower))

        # Determine language
        if has_ukrainian_chars or ukrainian_score > max(
            russian_score, english_score
        ):
            return "ua"
        elif has_cyrillic or russian_score > max(
            ukrainian_score, english_score
        ):
            return "ru"
        elif english_score > 0:
            return "en"
        else:
            # Default to English if unclear
            return "en"

    def detect_positive_sentiment(
        self, text: str, language: str = "en"
    ) -> Dict:
        """Detects if a message has positive sentiment towards meeting/coming to the conference"""
        if not text:
            return {"sentiment": "neutral", "confidence": 0.0, "keywords": []}

        text_lower = text.lower()
        matched_keywords = []

        if language == "en":
            positive_keywords = [
                "yes",
                "sure",
                "definitely",
                "absolutely",
                "of course",
                "great",
                "sounds good",
                "perfect",
                "excellent",
                "wonderful",
                "amazing",
                "awesome",
                "love to",
                "would love",
                "interested",
                "looking forward",
                "excited",
                "can't wait",
                "see you there",
                "meet you",
                "connect",
                "booth",
                "stand",
                "visit",
                "come by",
                "stop by",
                "let's meet",
                "let's connect",
                "schedule",
                "appointment",
                "time to chat",
                "happy to",
                "glad to",
                "pleasure",
                "honor",
                "thrilled",
            ]
            negative_keywords = [
                "no",
                "not interested",
                "busy",
                "can't",
                "cannot",
                "won't",
                "will not",
                "unable",
                "unavailable",
                "sorry",
                "unfortunately",
                "maybe next time",
                "not available",
                "declined",
                "pass",
                "skip",
                "not now",
                "later",
            ]

        elif language == "ua":  # Ukrainian
            positive_keywords = [
                "так",
                "звичайно",
                "обов'язково",
                "з радістю",
                "з задоволенням",
                "чудово",
                "відмінно",
                "прекрасно",
                "класно",
                "супер",
                "буду",
                "приїжджу",
                "прийду",
                "зустрінемося",
                "побачимося",
                "цікаво",
                "цікавлюсь",
                "хочу",
                "можу",
                "зможу",
            ]
            negative_keywords = [
                "ні",
                "не можу",
                "не зможу",
                "зайнятий",
                "зайнята",
                "на жаль",
                "шкода",
                "не цікаво",
                "не буду",
                "не приїжджаю",
                "не прийду",
                "пізніше",
                "може наступного разу",
            ]

        elif language == "ru":  # Russian
            positive_keywords = [
                "да",
                "конечно",
                "обязательно",
                "с радостью",
                "с удовольствием",
                "отлично",
                "прекрасно",
                "классно",
                "супер",
                "буду",
                "приеду",
                "приду",
                "встретимся",
                "увидимся",
                "интересно",
                "интересуюсь",
                "хочу",
                "могу",
                "смогу",
            ]
            negative_keywords = [
                "нет",
                "не могу",
                "не смогу",
                "занят",
                "занята",
                "к сожалению",
                "жаль",
                "не интересно",
                "не буду",
                "не приеду",
                "не приду",
                "позже",
                "может в следующий раз",
            ]

        else:  # Default to English
            positive_keywords = [
                "yes",
                "sure",
                "definitely",
                "absolutely",
                "of course",
                "great",
                "sounds good",
                "perfect",
                "excellent",
                "wonderful",
                "amazing",
                "awesome",
                "love to",
                "would love",
                "interested",
                "looking forward",
                "excited",
                "can't wait",
                "see you there",
            ]
            negative_keywords = [
                "no",
                "not interested",
                "busy",
                "can't",
                "cannot",
                "won't",
                "will not",
                "unable",
                "unavailable",
                "sorry",
                "unfortunately",
                "maybe next time",
            ]

        # Check for positive keywords
        for keyword in positive_keywords:
            if keyword in text_lower:
                matched_keywords.append(keyword)

        # Check for negative keywords (they override positive)
        negative_matches = []
        for keyword in negative_keywords:
            if keyword in text_lower:
                negative_matches.append(keyword)

        # Calculate sentiment
        if negative_matches:
            return {
                "sentiment": "negative",
                "confidence": 0.8,
                "keywords": negative_matches,
                "positive_keywords": matched_keywords,
            }
        elif matched_keywords:
            return {
                "sentiment": "positive",
                "confidence": min(0.9, 0.3 + len(matched_keywords) * 0.2),
                "keywords": matched_keywords,
                "negative_keywords": [],
            }
        else:
            return {"sentiment": "neutral", "confidence": 0.5, "keywords": []}

    def check_message_already_sent_in_chat(
        self, chat_data: dict, followup_type: str, accounts: dict = None
    ) -> bool:
        """Перевіряє чи вже було відправлено повідомлення цього типу в чаті"""
        if not chat_data or not isinstance(chat_data, dict):
            return False

        messages = chat_data.get("messages", [])
        if not messages:
            return False

        # Get current user ID to identify our messages
        if accounts and self.base_scraper.current_account in accounts:
            current_user_id = accounts[self.base_scraper.current_account][
                "user_id"
            ]
        else:
            # Fallback: try to get from any message in the chat
            return False

        # Get the template for this followup type
        if followup_type not in self.follow_up_templates:
            return False

        template = self.follow_up_templates[followup_type]

        # Extract key phrases to check for
        key_phrases = []
        if isinstance(template, dict):
            # Multi-language template
            for lang_template in template.values():
                # Extract distinctive phrases (without {name} placeholder)
                clean_template = lang_template.replace("{name}", "").strip()
                if len(clean_template) > 10:  # Only use substantial phrases
                    key_phrases.append(
                        clean_template[:30]
                    )  # First 30 chars as key phrase
        else:
            # Single language template
            clean_template = template.replace("{name}", "").strip()
            if len(clean_template) > 10:
                key_phrases.append(clean_template[:30])

        # Check our messages for these key phrases
        for msg in messages:
            if msg.get("userId") == current_user_id and msg.get("message"):
                message_text = msg.get("message", "")
                for phrase in key_phrases:
                    if phrase.lower() in message_text.lower():
                        return True

        return False

    def check_followup_already_sent(
        self,
        csv_file: str,
        chat_id: str,
        followup_type: str,
        chat_data: dict = None,
        accounts: dict = None,
    ) -> bool:
        """Перевіряє чи вже був відправлений follow-up цього типу з покращеною двохрівневою логікою"""
        try:
            import pandas as pd

            # TIER 1: Швидка перевірка CSV (primary defense)
            df = pd.read_csv(csv_file)

            # Знаходимо рядок з цим chat_id
            chat_row = df[df["chat_id"] == chat_id]

            csv_says_sent = False
            if not chat_row.empty:
                # Перевіряємо статус цього followup_type
                if followup_type == "conference_active":
                    column_name = "Conference Active Status"
                else:
                    # Check both new column format and legacy format
                    column_name = f"Follow_up_{followup_type}_status"
                    if column_name not in df.columns:
                        # Fallback to legacy "Follow-up type" column
                        followup_type_col = chat_row["Follow-up type"].iloc[0]
                        if pd.notna(
                            followup_type_col
                        ) and followup_type in str(followup_type_col):
                            csv_says_sent = True

                if column_name in df.columns and not csv_says_sent:
                    status = chat_row[column_name].iloc[0]
                    if pd.notna(status) and str(status).lower() in [
                        "sent",
                        "true",
                        "1",
                    ]:
                        csv_says_sent = True

            # Якщо CSV каже, що відправлено - довіряємо йому (оптимізація)
            if csv_says_sent:
                return True

            # TIER 2: Перевірка фактичного контенту повідомлень (secondary defense)
            if chat_data:
                message_says_sent = self.check_message_already_sent_in_chat(
                    chat_data, followup_type, accounts
                )

                # Якщо повідомлення показують, що було відправлено, але CSV не знає про це
                if message_says_sent and not csv_says_sent:
                    print(
                        f"       🔄 Знайдено розбіжність: повідомлення показують відправлено, але CSV ні"
                    )
                    print(f"       📝 Оновлюємо CSV для синхронізації...")

                    # Синхронізуємо CSV з фактичним станом повідомлень
                    self.data_processor.update_csv_followup_status(
                        csv_file, chat_id, followup_type, chat_data
                    )

                return message_says_sent

            # Якщо немає даних чату, довіряємо CSV
            return csv_says_sent

        except ImportError:
            print(f"       ⚠️ Pandas не доступний, пропускаємо CSV перевірку")
            # Fallback до перевірки повідомлень якщо pandas недоступний
            if chat_data:
                return self.check_message_already_sent_in_chat(
                    chat_data, followup_type, accounts
                )
            return False
        except Exception as e:
            print(f"       ⚠️ Помилка перевірки follow-up: {e}")
            # В разі помилки, якщо є дані чату - перевіряємо їх
            if chat_data:
                return self.check_message_already_sent_in_chat(
                    chat_data, followup_type, accounts
                )
            return False

    def analyze_chat_for_responses(self, chat_data: Dict, accounts) -> Dict:
        """Analyzes chat for participant responses"""
        result = {
            "has_response": False,
            "participant_name": None,
            "participant_id": None,
            "response_count": 0,
            "first_response_date": None,
        }

        if not chat_data or not isinstance(chat_data, dict):
            return result

        messages = chat_data.get("messages", [])
        if not messages:
            return result

        # Get current user ID
        current_user_id = accounts[self.base_scraper.current_account][
            "user_id"
        ]

        # Get chat participant information
        if chat_data.get("isSingleChat") and chat_data.get("participants"):
            participants = chat_data.get("participants", [])
            for participant in participants:
                if participant.get("userId") != current_user_id:
                    result["participant_id"] = participant.get("userId")
                    result["participant_name"] = (
                        f"{participant.get('firstName', '')} {participant.get('lastName', '')}".strip()
                    )
                    break

        # Sort messages by time
        sorted_messages = sorted(
            messages, key=lambda x: x.get("createdDate", "")
        )

        # Find responses from participant (not from us)
        response_messages = []
        for msg in sorted_messages:
            if msg.get("userId") != current_user_id:
                response_messages.append(msg)

        if response_messages:
            result["has_response"] = True
            result["response_count"] = len(response_messages)

            # Get first response date
            first_response = response_messages[0]
            result["first_response_date"] = self.parse_message_timestamp(
                first_response.get("createdDate", "")
            )

        return result

    def process_positive_conversation_followups(
        self, csv_file: str, accounts
    ) -> Dict[str, int]:
        """Processes positive conversations across all messaging accounts and sends conference followup"""
        print(f"\n📬 ОБРОБКА ПОЗИТИВНИХ РОЗМОВ ДЛЯ CONFERENCE FOLLOWUP")
        print(f"📁 CSV файл: {csv_file}")
        print("=" * 60)

        stats = {
            "total_accounts": 0,
            "total_chats_checked": 0,
            "positive_conversations": 0,
            "conference_followups_sent": 0,
            "already_sent": 0,
            "errors": 0,
            "language_detected": {"en": 0, "ua": 0, "ru": 0, "unknown": 0},
            "sentiment_analysis": {"positive": 0, "negative": 0, "neutral": 0},
            "accounts_processed": [],
        }

        # List of messenger accounts to check
        messenger_accounts = ["messenger1", "messenger2", "messenger3"]
        original_account = self.base_scraper.current_account

        try:
            for account_key in messenger_accounts:
                if account_key not in accounts:
                    continue

                print(f"\n🔄 Переключаємося на {account_key}...")
                if not self.base_scraper.switch_account(account_key, accounts):
                    print(f"❌ Не вдалося переключитись на {account_key}")
                    continue

                stats["accounts_processed"].append(account_key)
                stats["total_accounts"] += 1

                # Load chat list for this account
                chats_data = self.load_chats_list(accounts)
                if not chats_data:
                    continue

                for chat in chats_data:
                    if not chat.get("isSingleChat"):
                        continue

                    chat_id = chat.get("chatId")
                    if not chat_id:
                        continue

                    stats["total_chats_checked"] += 1

                    # Load detailed chat data
                    chat_details = self.load_chat_details(chat_id)
                    if not chat_details:
                        continue

                    # Analyze for responses
                    response_analysis = self.analyze_chat_for_responses(
                        chat_details, accounts
                    )

                    if response_analysis["has_response"]:
                        messages = chat_details.get("messages", [])

                        # Analyze sentiment of responses
                        current_user_id = accounts[
                            self.base_scraper.current_account
                        ]["user_id"]
                        participant_messages = [
                            msg
                            for msg in messages
                            if msg.get("userId") != current_user_id
                        ]

                        has_positive = False
                        for msg in participant_messages:
                            text = msg.get("message", "")
                            if not text:
                                continue

                            language = self.detect_language(text)
                            stats["language_detected"][language] += 1

                            sentiment = self.detect_positive_sentiment(
                                text, language
                            )
                            stats["sentiment_analysis"][
                                sentiment["sentiment"]
                            ] += 1

                            if sentiment["sentiment"] == "positive":
                                has_positive = True

                        if has_positive:
                            stats["positive_conversations"] += 1

                            # Check if conference followup already sent
                            already_sent = self.check_followup_already_sent(
                                csv_file,
                                chat_id,
                                "conference_active",
                                chat_details,
                            )

                            if already_sent:
                                stats["already_sent"] += 1
                            else:
                                # Send conference followup
                                participant_name = response_analysis.get(
                                    "participant_name", ""
                                )
                                language = self.detect_language(
                                    participant_messages[-1].get("message", "")
                                )

                                success = self.send_followup_message(
                                    chat_id,
                                    "conference_active",
                                    participant_name,
                                    language,
                                )

                                if success:
                                    stats["conference_followups_sent"] += 1
                                    # Update CSV
                                    self.data_processor.update_csv_followup_status(
                                        csv_file, chat_id, "conference_active"
                                    )
                                else:
                                    stats["errors"] += 1

        except Exception as e:
            print(f"❌ Помилка в обробці позитивних розмов: {e}")
            stats["errors"] += 1

        finally:
            # Restore original account
            if (
                original_account
                and original_account != self.base_scraper.current_account
            ):
                self.base_scraper.switch_account(original_account, accounts)

        # Print final summary
        print(f"\n📊 ПІДСУМКИ CONFERENCE FOLLOWUP КАМПАНІЇ:")
        print(f"   👥 Акаунтів оброблено: {stats['total_accounts']}")
        print(f"   📬 Чатів перевірено: {stats['total_chats_checked']}")
        print(f"   ✅ Позитивних розмов: {stats['positive_conversations']}")
        print(
            f"   📨 Conference followup відправлено: {stats['conference_followups_sent']}"
        )
        print(f"   ⏭️ Вже відправлені: {stats['already_sent']}")
        print(f"   ❌ Помилок: {stats['errors']}")
        print(f"   🔧 Акаунти: {', '.join(stats['accounts_processed'])}")

        print(f"\n🌍 СТАТИСТИКА МОВИ:")
        for lang, count in stats["language_detected"].items():
            print(f"   {lang.upper()}: {count}")

        print(f"\n😊 СТАТИСТИКА СЕНТИМЕНТУ:")
        for sentiment, count in stats["sentiment_analysis"].items():
            print(f"   {sentiment.capitalize()}: {count}")

        success_rate = (
            (
                (
                    stats["conference_followups_sent"]
                    / max(stats["positive_conversations"], 1)
                )
                * 100
            )
            if stats["positive_conversations"] > 0
            else 0
        )
        print(f"   📈 Успішність відправки: {success_rate:.1f}%")

        return stats

    def check_all_responses_and_update_csv(
        self, csv_file: str, accounts
    ) -> Dict[str, int]:
        """Checks all chats from all accounts for responses and updates CSV status
        Optimized version - only checks chats for contacts with Sent/Empty/True status
        """
        print(f"\n📬 ОПТИМІЗОВАНА ПЕРЕВІРКА ВІДПОВІДЕЙ У ЧАТАХ")
        print(f"📁 CSV файл: {csv_file}")
        print("=" * 60)

        stats = {
            "total_accounts": 0,
            "total_chats_to_check": 0,
            "total_chats_checked": 0,
            "responses_found": 0,
            "csv_updated": 0,
            "errors": 0,
            "skipped_no_csv_match": 0,
            "skipped_already_answered": 0,
            "accounts_processed": [],
        }

        # List of messenger accounts to check
        messenger_accounts = ["messenger1", "messenger2", "messenger3"]
        original_account = self.base_scraper.current_account

        try:
            # Load CSV to get relevant contacts
            relevant_chat_ids = (
                self.data_processor._get_relevant_chat_ids_from_csv(csv_file)
            )
            stats["total_chats_to_check"] = len(relevant_chat_ids)

            for account_key in messenger_accounts:
                if account_key not in accounts:
                    continue

                print(f"\n🔄 Переключаємося на {account_key}...")
                if not self.base_scraper.switch_account(account_key, accounts):
                    print(f"❌ Не вдалося переключитись на {account_key}")
                    continue

                stats["accounts_processed"].append(account_key)
                stats["total_accounts"] += 1

                # Load accessible chats for current account
                chats_data = self.load_chats_list(accounts)
                if not chats_data:
                    continue

                accessible_chat_ids = {
                    chat.get("chatId")
                    for chat in chats_data
                    if chat.get("chatId")
                }

                # Check only relevant chats that are accessible
                for chat_id in relevant_chat_ids:
                    if chat_id not in accessible_chat_ids:
                        continue

                    stats["total_chats_checked"] += 1

                    try:
                        # Load chat details
                        chat_details = self.load_chat_details(chat_id)
                        if not chat_details:
                            continue

                        # Analyze for responses
                        response_analysis = self.analyze_chat_for_responses(
                            chat_details, accounts
                        )

                        if response_analysis["has_response"]:
                            stats["responses_found"] += 1

                            # Update CSV
                            success = self.data_processor.update_csv_response_status_by_chat_id(
                                csv_file,
                                chat_id,
                                True,
                                response_analysis["participant_name"],
                                response_analysis["participant_id"],
                            )

                            if success:
                                stats["csv_updated"] += 1

                    except Exception as e:
                        print(f"⚠️ Помилка обробки чату {chat_id}: {e}")
                        stats["errors"] += 1

        except Exception as e:
            print(f"❌ Критична помилка: {e}")
            stats["errors"] += 1

        # Print summary
        print(f"\n📊 ПІДСУМКИ ОПТИМІЗОВАНОЇ ПЕРЕВІРКИ ВІДПОВІДЕЙ:")
        print(f"   👥 Акаунтів перевірено: {stats['total_accounts']}")
        print(f"   🎯 Чатів для перевірки: {stats['total_chats_to_check']}")
        print(
            f"   📬 Чатів фактично перевірено: {stats['total_chats_checked']}"
        )
        print(f"   ✅ Відповідей знайдено: {stats['responses_found']}")
        print(f"   📝 CSV записів оновлено: {stats['csv_updated']}")
        print(f"   ❌ Помилок: {stats['errors']}")
        print(f"   🔧 Акаунти: {', '.join(stats['accounts_processed'])}")

        efficiency_rate = (
            (stats["total_chats_checked"] / stats["total_chats_to_check"])
            * 100
            if stats["total_chats_to_check"] > 0
            else 0
        )
        print(f"   📈 Ефективність фільтрації: {efficiency_rate:.1f}%")

        return stats
