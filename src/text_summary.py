# Text Summarization
# Algorithm based on weighting
import re
import sys
import nltk
from nltk.tokenize import RegexpTokenizer, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import bs4 as bs
import requests
import heapq
import textwrap

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

stopwords = stopwords.words('english')
tokenizer = RegexpTokenizer(r'\w+')
stemmer = SnowballStemmer('english')

def preprocessing(text):

    # Removing special characters and digits
    #text = re.sub('[^a-zA-Z]', ' ', text )
    text = re.sub(r'\s+', ' ', text)

    return text.lower()


def fetch_url(url):

    res = requests.get(url)

    parsed_webpage = bs.BeautifulSoup(res.content,'lxml')

    paragraphs = parsed_webpage.find_all('p')

    text = ""

    for p in paragraphs:
        text += p.text

    return text


def read_file(fname):

    with open(fname) as f:
        content = f.readlines()

    # remove whitespace characters like `\n` at the end of each line
    content = [x.strip().lower() for x in content]

    return content


def text_summary(text, numlines=7, lang='english'):

    # Preprocessing
    text = preprocessing(text)

    # tokenize and form nltk objects
    tokens = nltk.Text(tokenizer.tokenize(text))

    # remove punctuations and digits and change to lower case
    tokens = [w.lower() for w in tokens if w.isalpha() and not w.isdigit()]

    # remove stop words and stem the words
    tokens = [stemmer.stem(w) for w in tokens if w not in stopwords]

    # find word frequencies
    tokens_frequencies = dict(nltk.FreqDist(tokens))

    # split document into sentences
    sentences = sent_tokenize(text)

    # calculate score for every sentence
    sentence_scores = dict((sent,0) for sent in sentences)
    for sent in sentences:
        if len(sent.split()) < 80:
            # find all words in the sentence
            words = tokenizer.tokenize(sent)
            words = [stemmer.stem(w.lower()) for w in words if w.isalpha() and not w.isdigit()]

            # sum of term frequencies
            for word in words:
                sentence_scores[sent] += tokens_frequencies.get(word, 0)

    summary_sentences = heapq.nlargest(numlines, sentence_scores, key=sentence_scores.get)

    summary = '\n'.join(summary_sentences)

    return summary


if __name__ == '__main__':

    numlines = 5
    url = ''
    text = fetch_url(url)

    summary = text_summary(text, numlines)

    print('\n'.join(textwrap.wrap(summary, 80, break_long_words=False)))

    sys.exit(0)



