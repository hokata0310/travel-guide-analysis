--First visitors
CREATE TABLE first_visitors 
(
  number       VARCHAR,
  stamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  event_type   TEXT,
  country      TEXT,
  user_id      VARCHAR,
  source       TEXT,
  topic        TEXT
);

COPY first_visitors
FROM '/home/hkata/dilans_data/first_visitors.csv' DELIMITER ';' CSV HEADER;

--Returning visitors
CREATE TABLE returning_visitors 
(
  number       VARCHAR,
  stamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  event_type   TEXT,
  country      TEXT,
  user_id      VARCHAR,
  topic        TEXT
);

COPY returning_visitors
FROM '/home/hkata/dilans_data/returning_visitors.csv' DELIMITER ';' CSV HEADER;

--Subscribers
CREATE TABLE subscribes 
(
  stamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  event_type   TEXT,
  user_id      VARCHAR
);

COPY subscribes
FROM '/home/hkata/dilans_data/subscribes.csv' DELIMITER ';';

--Purchase
CREATE TABLE purchase 
(
  stamp        TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  event_type   TEXT,
  user_id      VARCHAR,
  price        INTEGER
);

COPY purchase
FROM '/home/hkata/dilans_data/buys.csv' DELIMITER ';';

COMMIT;

--Readers by source
SELECT source,
       COUNT(*) AS first_readers_by_source
FROM first_visitors
GROUP BY source
ORDER BY first_readers_by_source DESC;

--Readers by country
SELECT country,
       COUNT(*) AS first_readers_by_country
FROM first_visitors
GROUP BY country
ORDER BY first_readers_by_country DESC;

--Readers by topic
SELECT topic,
       COUNT(*) AS first_readers_by_topic
FROM first_visitors
GROUP BY topic
ORDER BY first_readers_by_topic DESC;

--Returners by source 
SELECT source,
       COUNT(DISTINCT returning_visitors.user_id) AS returning_readers_by_source
FROM returning_visitors
  JOIN first_visitors ON returning_visitors.user_id = first_visitors.user_id
GROUP BY source
ORDER BY returning_readers_by_source DESC;

--Returners by country
SELECT first_visitors.country,
       COUNT(DISTINCT returning_visitors.user_id) AS returning_readers_by_country
FROM returning_visitors
  JOIN first_visitors ON returning_visitors.user_id = first_visitors.user_id
GROUP BY first_visitors.country
ORDER BY returning_readers_by_country DESC;

--Returners by topic
SELECT first_visitors.topic,
       COUNT(DISTINCT returning_visitors.user_id) AS returning_readers_by_topic
FROM returning_visitors
  JOIN first_visitors ON returning_visitors.user_id = first_visitors.user_id
GROUP BY first_visitors.topic
ORDER BY returning_readers_by_topic DESC;

--Subscribers by source 
SELECT source,
       COUNT(DISTINCT subscribes.user_id) AS subscribers_by_source
FROM subscribes
  JOIN first_visitors ON subscribes.user_id = first_visitors.user_id
GROUP BY source
ORDER BY subscribers_by_source DESC;

--Subscribers by country
SELECT first_visitors.country,
       COUNT(DISTINCT subscribes.user_id) AS subscribers_by_country
FROM subscribes
  JOIN first_visitors ON subscribes.user_id = first_visitors.user_id
GROUP BY first_visitors.country
ORDER BY subscribers_by_country DESC;

--Subscribers by topic
SELECT first_visitors.topic,
       COUNT(DISTINCT subscribes.user_id) AS subscribers_by_topic
FROM subscribes
  JOIN first_visitors ON subscribes.user_id = first_visitors.user_id
GROUP BY first_visitors.topic
ORDER BY subscribers_by_topic DESC;

--Buyers by source 
SELECT source,
       COUNT(DISTINCT purchase.user_id) AS purchase_by_source
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY source
ORDER BY purchase_by_source DESC;

--Buyers by country
SELECT first_visitors.country,
       COUNT(DISTINCT purchase.user_id) AS purchase_by_country
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY first_visitors.country
ORDER BY purchase_by_country DESC;

--Buyers by topic
SELECT first_visitors.topic,
       COUNT(DISTINCT purchase.user_id) AS purchase_by_topic
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY first_visitors.topic
ORDER BY purchase_by_topic DESC;

--Funnel
SELECT first_readers.dates,
       first_readers.country,
       first_readers.source,
       first_readers.topic,
       first_readers.first_visitors,
       returning_readers.returning_visitor,
       subscribers.subscribed,
       buyers.buyer
FROM
--#1 First readers
(SELECT DATE (stamp) AS dates,
        country,
        source,
        topic,
        COUNT(*) AS first_visitors
 FROM first_visitors
 GROUP BY dates,
          country,
          source,
          topic
 ORDER BY dates) AS first_readers
  LEFT JOIN
--#2 Returning readers
 (SELECT DATE (first_visitors.stamp) AS dates,
         first_visitors.country,
         first_visitors.source,
         first_visitors.topic,
         COUNT(DISTINCT (returning_visitors.user_id)) AS returning_visitor
  FROM returning_visitors
    JOIN first_visitors ON returning_visitors.user_id = first_visitors.user_id
  GROUP BY dates,
           first_visitors.country,
           first_visitors.source,
           first_visitors.topic
  ORDER BY dates) AS returning_readers
         ON first_readers.dates = returning_readers.dates
        AND first_readers.country = returning_readers.country
        AND first_readers.source = returning_readers.source
        AND first_readers.topic = returning_readers.topic
  LEFT JOIN
--#3 Subscribers
 (SELECT DATE (first_visitors.stamp) AS dates,
         first_visitors.country,
         first_visitors.source,
         first_visitors.topic,
         COUNT(DISTINCT (subscribes.user_id)) AS subscribed
  FROM subscribes
    JOIN first_visitors ON subscribes.user_id = first_visitors.user_id
  GROUP BY dates,
           first_visitors.country,
           first_visitors.source,
           first_visitors.topic
  ORDER BY dates) AS subscribers
         ON first_readers.dates = subscribers.dates
        AND first_readers.country = subscribers.country
        AND first_readers.source = subscribers.source
        AND first_readers.topic = subscribers.topic
  LEFT JOIN
--#4 Buyers
 (SELECT DATE (first_visitors.stamp) AS dates,
         first_visitors.country,
         first_visitors.source,
         first_visitors.topic,
         COUNT(DISTINCT (purchase.user_id)) AS buyer
  FROM purchase
    JOIN first_visitors ON purchase.user_id = first_visitors.user_id
  GROUP BY dates,
           first_visitors.country,
           first_visitors.source,
           first_visitors.topic
  ORDER BY dates) AS buyers
         ON first_readers.dates = buyers.dates
        AND first_readers.country = buyers.country
        AND first_readers.source = buyers.source
        AND first_readers.topic = buyers.topic;

--Number of purchases by buyers
SELECT user_id,
       COUNT(user_id) AS purchase_per_user
FROM purchase
GROUP BY user_id
ORDER BY purchase_per_user DESC;

--Microsegmentation by the number of buyers
SELECT first_visitors.source,
       first_visitors.country,
       first_visitors.topic,
       COUNT(DISTINCT (purchase.user_id)) AS buyers
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY first_visitors.source,
         first_visitors.country,
         first_visitors.topic
ORDER BY buyers DESC;

--Microsegmentation by revenue
SELECT first_visitors.source,
       first_visitors.country,
       first_visitors.topic,
       SUM(price) AS rev
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY first_visitors.source,
         first_visitors.country,
         first_visitors.topic
ORDER BY rev_by_source DESC;

--Number of e-book buyers (spent 8$)
SELECT COUNT(*)
FROM (SELECT user_id,
             SUM(price) AS rev
      FROM purchase
      GROUP BY user_id) AS revenue_by_buyer
WHERE rev = 8;

--Number of training buyers (spent 80$)
SELECT COUNT(*)
FROM (SELECT user_id,
             SUM(price) AS rev
      FROM purchase
      GROUP BY user_id) AS revenue_by_buyer
WHERE rev = 80;

--Number of returning buyers (spent 88$)
SELECT COUNT(*)
FROM (SELECT user_id,
             SUM(price) AS rev
      FROM purchase
      GROUP BY user_id) AS revenue_by_buyer
WHERE rev = 88;

--Returning buyers/Buyers ratio (1759/6648=0,2646 > 26,46%)
SELECT COUNT(*)
FROM (SELECT user_id,
             SUM(price) AS rev
      FROM purchase
      GROUP BY user_id) AS revenue_by_buyer
WHERE rev = 88;

SELECT COUNT(DISTINCT (user_id)) AS buyers
FROM purchase;

--Revenue by source
SELECT source,
       SUM(price) AS rev_by_source
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY source
ORDER BY rev_by_source DESC;

--Revenue by country 
SELECT country,
       SUM(price) AS rev_by_country
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY country
ORDER BY rev_by_country DESC;

--Revenue by topic
SELECT topic,
       SUM(price) AS rev_by_topic
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY topic
ORDER BY rev_by_topic DESC;

--Daily revenue
SELECT DATE (purchase.stamp) AS dates,
       SUM(price) AS daily_rev
FROM purchase
  JOIN first_visitors ON purchase.user_id = first_visitors.user_id
GROUP BY dates;

