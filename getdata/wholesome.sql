-- This is all to be run in google bigquery

select author from [fh-bigquery:reddit_comments.all]
  where subreddit = 'wholesomememes' and author != '[deleted]'
  group by author;
  -- into wholesome.authors

select author,
       regexp_replace(body, r'\r\n|\n\r|\r|\n|"', ' ') as body,
       subreddit,
       created_utc
  from [fh-bigquery:reddit_comments.all]
  where author in (select author from [wholesome.authors])
    and body != '[deleted]';
    -- into wholesome.comments
