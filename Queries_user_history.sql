-- 1. List all unique email addresses.
SELECT DISTINCT email 
FROM user_history;

-- 2. Count the number of emails received per day.
SELECT date(date) AS day, COUNT(*) AS email_count
FROM user_history
GROUP BY day
ORDER BY day;

-- 3. Find the first and last email date for each email address.
SELECT email, 
       MIN(date) AS first_email_date, 
       MAX(date) AS last_email_date
FROM user_history
GROUP BY email;

-- 4. Count the total number of emails from each domain (e.g., gmail.com, yahoo.com).
SELECT SUBSTRING_INDEX(email, '@', -1) AS domain, COUNT(*) AS email_count
FROM user_history
GROUP BY domain
ORDER BY email_count DESC;

-- 5. List the top 5 email addresses with the highest number of emails.
SELECT email, COUNT(*) AS email_count
FROM user_history
GROUP BY email
ORDER BY email_count DESC
LIMIT 5;

-- 6. List the top 5 days with the highest number of emails received.
SELECT date(date) AS day, COUNT(*) AS email_count
FROM user_history
GROUP BY day
ORDER BY email_count DESC
LIMIT 5;

-- 7. Find the average number of emails received per day.
SELECT AVG(daily_count) AS average_emails_per_day
FROM (
    SELECT COUNT(*) AS daily_count
    FROM user_history
    GROUP BY date(date)
) AS daily_counts;

-- 8. Count the number of unique email addresses for each day.
SELECT date(date) AS day, COUNT(DISTINCT email) AS unique_emails
FROM user_history
GROUP BY day
ORDER BY day;

-- 9. Find the email address that received the most emails on a single day.
SELECT email, date(date) AS day, COUNT(*) AS email_count
FROM user_history
GROUP BY email, day
ORDER BY email_count DESC
LIMIT 1;

-- 10. Find the average interval (in days) between emails for each email address.
SELECT email, 
       AVG(diff_days) AS avg_interval_days
FROM (
    SELECT email, 
           DATEDIFF(LEAD(date, 1, date) OVER (PARTITION BY email ORDER BY date), date) AS diff_days
    FROM user_history
) AS intervals
GROUP BY email;