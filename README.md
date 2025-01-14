# Telegram Job Data Collection Bot

### Description
A Python-based bot designed to monitor Telegram channels for job postings, process the collected data, and store it in a database for further analysis.

### Features

1. **Telegram Channel Monitoring**
   - Tracks multiple Telegram channels (e.g., `UstozShogird`, `uzdev_jobs`, `itjobstashkent`, etc.) for new messages.

2. **Data Filtering**
   - Detects and skips duplicate messages based on unique parameters (source, date, text).
   - Filters messages to ensure they match job vacancy criteria.

3. **Data Processing**
   - Cleans and standardizes message text.
   - Extracts job details such as position, company, location, salary, and more.
   - Converts salaries to USD.
   - Categorizes job data (e.g., programming languages, tech stack).

4. **Database Integration**
   - Stores key vacancy details, such as location, company, experience, source, salary, and date.
   - Saves raw Telegram message data, including message ID and text.
   - Inserts associated programming languages and tech stacks.

5. **Logging**
   - Logs every operation, including message processing, filtering, database insertion, and errors, for transparency and debugging.

### Technologies

- **Python**
- **Telethon**: For interaction with Telegram API.
- **Logging**: For detailed operation tracking.
- **SQL**: For data storage and retrieval.
- **Custom Scripts**:
  - Filtering and cleaning data.
  - Extracting and categorizing job-related details.

### Usage Example

The bot connects to Telegram, retrieves messages from predefined channels, filters and processes the data, stores unique job postings in the database, and logs all actions.

This tool is ideal for automating the monitoring of job postings, analyzing data, and generating reports.
