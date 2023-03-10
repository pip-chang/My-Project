# Reddit Corpus Analysis

## Purpose of the Project

This is a project I started to aid my qualitative anthropological research of highly active and diverse Reddit communities in terms of how they sustain themselves as a system, and how different users interact with the community and experience a sense of belonging.

It is also a way for me to learn data collection methods and analyzing skills that I will need in my thesis research. Through this learning process, I've picked up various methods to scrape readily available data with python scripts, whether be it using an API or building a crawler. I've improved my programming skills and increased my proficiency in useful python libraries for data analysis and visualization. Furthermore, I came to discover exploratory data analysis strategies based on corpus analysis and natural language processing, which serve to reveal insights and features of interest that aren't so visible when approached with traditional close reading methods. Such strategies can help me explore and assess datasets more efficiently, and make more informed decisions when selecting methods for my research questions, or identify new questions.

## Code Description

1. Collecting & Storing Data

In the script `"collect_data.py"`, I used [PSAW](https://github.com/dmarx/psaw)[^1], a python wrapper for the PushShift API, to collect all submissions and comments in the Subreddit r/TooAfraidToAsk from its conception to December 2020.

In each run of the script, the data is collected in chunks from earliest date to latest, and written into jsonlines files, each line being one submission or comment with its metadata based on the parameters. It keeps track of the latest data collected using its epoch, and starts from where it's left off in the next run. Each jsonlines file is limited to maximum 500,000 lines for easier processing.

2. Cleaning & Processing Data

In the script `"clean_data.ipynb"`, I used pandas to check for jsonlines files with duplicated data, and drop duplicates.

In the script `"process_data"`, I created a class and its child class to handle data and process data.

The `DataHandler` class loads the data according to user input and creates a generator that can iterate over the data loaded. Its child class `DataProcessor` has different functions to process the data in different ways and export the results to the user specified path. Check the docstrings for what exactly they do.

In the script `main.py`, one can customize their input and run the above scripts to process data.

3. Inspecting & Visualizing Data

In the script `plot_results.ipynb`, processed data is loaded, modified and visualized using numpy, pandas, matplotlib and seaborn. 

Due to the various ways you can manipulate data easily with numpy and pandas, no concrete functions are created. If you don't change anything and run every cell in the script in order, it will generate graphs with the [example data](./example-data/), from which interesting observations can be made. See [example graphs](./example-graphs/) for graphs I generated using all the data. 

4. Building Desired Corpora

There are different ways one can build a corpus, usually dependent on with what tools one explores the corpus with. As I intend to explore the corpus using AntConc, a freeware corpus analysis toolkit, who can import txt files, the script `corpus.ipynb` can sample and filter needed data and export them in a txt file.

If you don't change anything and run every cell in the script in order, it will extract a certain number of non-mod-made comments from the sample data that are longer than 50 tokens containing at least one of the keywords "community, Subreddit, identity, belonging, group, people". (The process might take a few minutes.) I used regex to match all the different forms of the keywords. One can also use nltk to lemmatize the texts, but it is not really necessary here. Each txt files has no more than 20000 entries to ensure AntConc works more smoothly, even though it can handle up to 10 million words.

5. Exploring Corpora

When loaded into AntConc, the extracted corpus can be analyzed with corpus linguistics methods such as frequency lists, plotting, clustering, n-gram, collocation, keywords, wordcloud, etc. See [example graphs](./example-graphs/) for examples of analysis using AntConc.

The corpus can also be analyzed using NLP methods. In `nlp_example.ipynb`, the TF-IDF (Term Frequency - Inverse Document Frequency) of non-mod-made comments with 50-100 words including at least one of these phrases "this community", "this subreddit", "this place", "you guys", and "you all" is calculated. Looking at the top 5 words with the highest TI-IDF scores, we can observe that when users address their imagined community of the Subreddit, it seems to be in highly emotionally charged situations, with debates often occuring relating to real life issues. This is only a easy demonstration, of course more sophisticated measures can be taken like randomly selecting data within a specific topic to find more patterns. 

There are many more ways to analyze data with nlp methods, for example combining TF-IDF with n-grams to analyze political speeches, using sentiment analysis and topic modeling to characterize emotions in conversations, etc.

## Conclusion

As I intend to do conduct my thesis research using discourse analysis methods, this project has opened up more possibilities for me to more effectively collect and explore data. What is particularly useful, is the possibility to approach discourses on social media from a much more informed starting point.

## Notes

[^1]: PushShift API has expreienced a switchover recently, and it has caused the wrapper to stop working. However, [PMAW](https://github.com/mattpodolak/pmaw) is updated and is compatible with the current PushShift API.
