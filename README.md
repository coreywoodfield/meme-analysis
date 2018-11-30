# meme-analysis

## Purpose

This code was used for research on the long-term effects of emotionally charged memes
(shared on reddit) on consumers of those memes, through the lens of their written text
before and after joining communities where said memes were shared. LIWC and Empath
were used to analyze the sentiment of the texts, and the Julia HypothesisTests package
was used to analyze the results of LIWC and Empath, and Apache Spark was used to sort
the users.

## Organization

The sql in [getdata](getdata) was run on Google BigQuery to get user data.
The code in [sortdata](sortdata) is a Spark program for the sorting of comments by user.
BigQuery is unwilling to sort such large amounts of data, and Spark is well suited
to the task. [processdata](processdata) contains Julia code for preprocessing the data
(cleaning text and filtering out users with insufficient information) and for processing
the data (running it through Empath and running paired t-tests on the results)
