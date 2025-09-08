# SBC Website Scraper

A sophisticated web scraper for the SBC (Sports Betting Community) events website that extracts attendee, company, and exhibitor information while respecting login requirements and implementing intelligent duplicate detection.

## Features

- 🔐 **Login-protected scraping** - Handles authentication automatically
- 🔄 **Duplicate detection** - Automatically skips already scraped records
- 📞 **Automatic contact extraction** - Extracts emails, phones, social handles from introduction text
- 🔍 **Infinite scroll support** - Handles scroll-based pagination automatically
- 🎭 **Browser automation** - Uses Playwright for reliable scraping
- 📊 **CSV output** - Structured data export
- 🛡️ **Rate limiting** - Respectful request timing
- 🖥️ **Interactive mode** - User-friendly command-line interface
- 📝 **Comprehensive logging** - Detailed operation logs
- 🔍 **Smart scroll detection** - Detects when all content is loaded

## Project Structure

```
restricted/
├── main.py          # Main application entry point
├── tools.py         # Core scraping logic and browser automation
├── helpers.py       # Utility functions, selectors, and data structures
├── config.py        # Configuration management with Pydantic
├── .env            # Environment variables and credentials
└── README.md       # This documentation
```

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Install Playwright browsers:**
   ```bash
   playwright install chromium
   ```

3. **Configure credentials:**
   Edit the `.env` file and add your SBC website credentials:
   ```env
   SBC_USERNAME=your_actual_username
   SBC_PASSWORD=your_actual_password
   ```

## Configuration

### Environment Variables (.env)

```env
# SBC Website Credentials
SBC_USERNAME=your_username_here
SBC_PASSWORD=your_password_here

# Website URLs
SBC_LOGIN_URL=https://sbcevents.com/login
SBC_ATTENDEES_URL=https://sbcevents.com/attendees

# Output files
ATTENDEES_CSV_PATH=attendees.csv

# Browser settings
HEADLESS=True
BROWSER_TIMEOUT=30000
DELAY_BETWEEN_REQUESTS=2
```

## Usage

### Interactive Mode (Recommended)

```bash
python main.py --interactive
```

This launches an interactive menu where you can:
- Choose which page to scrape
- View supported pages
- Monitor progress in real-time

### Command Line Mode

**Scrape attendees:**
```bash
python main.py --page attendees
```

**Scrape with custom output file:**
```bash
python main.py --page attendees --output my_attendees.csv
```

**Run with browser GUI (for debugging):**
```bash
python main.py --page attendees --no-headless
```

**List all supported pages:**
```bash
python main.py --list-pages
```

### Available Options

```bash
python main.py --help
```

- `--page PAGE` - Specify page to scrape (attendees, companies, exhibitors)
- `--interactive` - Run in interactive mode
- `--list-pages` - Show all supported pages
- `--output FILE` - Custom output CSV file path
- `--headless` - Force headless browser mode
- `--no-headless` - Run browser with GUI (debugging)

## Data Structure

### Attendees Data

The scraper extracts the following information for each attendee:

| Field | Description |
|-------|-------------|
| `full_name` | Complete name of the attendee |
| `company_name` | Company or organization name |
| `position` | Job title or role |
| `linkedin_url` | LinkedIn profile URL |
| `facebook_url` | Facebook profile URL |
| `x_twitter_url` | X (Twitter) profile URL |
| `country` | Country of residence/work |
| `responsibility` | Area of responsibility |
| `gaming_vertical` | Gaming industry vertical |
| `organization_type` | Type of organization |
| `introduction` | Personal bio or introduction |
| `source_url` | Original profile page URL |
| `profile_image_url` | Profile picture URL |

## Architecture

### Core Components

1. **SBCScraper Class (`tools.py`)**
   - Browser automation and session management
   - Login handling and authentication
   - Page navigation and data extraction
   - Pagination and error handling

2. **Configuration Management (`config.py`)**
   - Pydantic-based settings
   - Environment variable loading
   - Type validation

3. **Helper Utilities (`helpers.py`)**
   - CSS selectors for different page elements
   - Data structures and validation
   - CSV file management
   - URL normalization

4. **Main Application (`main.py`)**
   - Command-line interface
   - Interactive mode
   - Orchestration and workflow management

### CSS Selectors

The scraper uses flexible CSS selectors that can be easily updated for different website layouts:

```python
# Example selectors from helpers.py
LOGIN_USERNAME_INPUT = "input[name='username'], input[type='email'], #username, #email"
ATTENDEE_ITEM = ".attendee-item, .participant-item, .delegate-item, .member-card"
PROFILE_NAME = "h1, .profile-name, .attendee-name, .full-name"
```

## Duplicate Detection

The scraper implements intelligent **enhanced duplicate detection** using name-company pairs:

1. **Enhanced matching** - Compares full name AND company name pairs (case-insensitive)
2. **Improved accuracy** - Allows same person at different companies while preventing true duplicates
3. **CSV integration** - Reads existing name-company pairs before scraping
4. **Skip mechanism** - Avoids re-scraping existing profiles based on the combination
5. **Incremental updates** - Only adds new unique name-company combinations
6. **Real-time tracking** - Updates existing pairs list during scroll sessions

### Benefits of Enhanced Duplicate Checking:
- **More precise**: Same person at different companies won't be skipped
- **Better data quality**: Prevents false positives from name-only matching
- **Business intelligence**: Captures career moves and multi-company affiliations
- **Case-insensitive**: Handles name/company variations gracefully

### Example:
- ✅ "John Smith" at "Company A" and "John Smith" at "Company B" = **2 separate records**
- ❌ "John Smith" at "Company A" and "John Smith" at "Company A" = **1 record (duplicate)**

The SBC website uses infinite scroll instead of traditional pagination. The scraper handles this automatically by:

1. **Progressive Loading** - Scrolls down and waits for new content to load
2. **Duplicate Detection** - Tracks already processed attendees to avoid re-scraping
3. **End Detection** - Recognizes when no new content is being loaded
4. **Smart Scrolling** - Uses multiple scroll methods for reliable content loading
5. **Loading Indicators** - Waits for loading spinners and indicators to complete

### Scroll Behavior

```python
# The scraper automatically:
# 1. Scrolls to the bottom of the visible content
# 2. Waits for new attendees to load
# 3. Processes only new attendees (skips already scraped)
# 4. Repeats until no new content is detected
# 5. Stops when reaching the end of the list
```

### Configuration Options

Control scroll behavior through environment variables:

```env
# Delay between scroll attempts (seconds)
DELAY_BETWEEN_REQUESTS=2

# Browser timeout for waiting operations
BROWSER_TIMEOUT=30000

# Run with visible browser for debugging scroll behavior
HEADLESS=False
```

## Rate Limiting and Ethics

- **Respectful delays** - Configurable delays between requests
- **Session management** - Maintains login state efficiently
- **Error handling** - Graceful failure recovery
- **Logging** - Transparent operation monitoring

## Troubleshooting

### Common Issues

1. **Login Failed**
   - Verify credentials in `.env` file
   - Check if website URL has changed
   - Ensure account is active

2. **No Data Found**
   - Website structure may have changed
   - Update CSS selectors in `helpers.py`
   - Run with `--no-headless` to debug

3. **Browser Timeout**
   - Increase `BROWSER_TIMEOUT` in `.env`
   - Check internet connection
   - Try running without headless mode

### Debug Infinite Scroll

**Test scroll functionality:**
```bash
python test_infinite_scroll.py
```

**Debug with visible browser:**
```bash
python main.py --page attendees --no-headless
```

**Check scroll behavior:**
- Monitor console logs for scroll attempts
- Watch for "No new data loaded" messages
- Look for loading indicator detection
- Verify content count changes after scrolling

### Logs

Check `sbc_scraper.log` for detailed operation logs.

## Extending the Scraper

### Adding New Page Types

1. **Add selectors** in `helpers.py`:
   ```python
   class NewPageSelectors:
       ITEM_LIST = ".new-item"
       ITEM_NAME = ".new-name"
       # ... more selectors
   ```

2. **Create data structure**:
   ```python
   @dataclass
   class NewPageData:
       name: str
       # ... other fields
   ```

3. **Implement scraper function** in `tools.py`:
   ```python
   async def scrape_new_page(page_url: str, csv_filepath: str) -> int:
       # Implementation
   ```

4. **Register in main.py**:
   ```python
   'new_page': {
       'url': settings.new_page_url,
       'csv_path': settings.new_page_csv_path,
       'scraper_func': scrape_new_page,
       'description': 'Scrape new page type'
   }
   ```

### Updating Selectors

If the website layout changes, update the selectors in `helpers.py`:

```python
# Old selector
ATTENDEE_NAME = ".attendee-name"

# Updated selector (add new options)
ATTENDEE_NAME = ".attendee-name, .participant-name, .new-name-class"
```

## Security Notes

- **Credentials** - Never commit `.env` file to version control
- **Rate limiting** - Respect website terms of service
- **User agent** - Uses realistic browser headers
- **Session handling** - Proper login/logout procedures

## Performance

- **Concurrent requests** - Single session to avoid overwhelming server
- **Memory usage** - Streams data to CSV files
- **Browser resources** - Automatic cleanup and resource management

## License

This project is for educational and authorized use only. Ensure compliance with the target website's terms of service and robots.txt file.

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review log files for error details
3. Test with `--no-headless` mode for debugging
4. Verify website structure hasn't changed
