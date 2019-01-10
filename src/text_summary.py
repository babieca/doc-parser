# Text Summarization
# Algorithm based on weighting
import re
import sys
import nltk
from nltk.tokenize import RegexpTokenizer, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
from subprocess import Popen, PIPE
import bs4 as bs
import requests
import heapq
import textwrap
import textract
import utils

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


def fetch_url(url):

    res = requests.get(url)

    parsed_webpage = bs.BeautifulSoup(res.content,'lxml')

    paragraphs = parsed_webpage.find_all('p')

    text = ""

    for p in paragraphs:
        text += p.text

    return text

def regex_srch(text, search):
    match = ''
    try:
        match = re.search(r'(?<='+search+').*', text).group().strip()
    except:
        return None
    else:
        return utils.remove_non_printable_chars(match)


def get_pdfinfo(pdf_path):
    proc = Popen(["pdfinfo", pdf_path], stdout=PIPE, stderr=PIPE)
    out, err = proc.communicate()
    data = out.decode("utf8", "ignore")
    return utils.input2num(regex_srch(data, 'Pages:'))


def read_pdf_file(filename, exclude_sent_with_words=[]):
    clean_text = ''
    text = textract.process(filename, encoding='utf-8')
    text = text.decode("utf-8")
    text = utils.remove_non_printable_chars(text)
    text = text.split('\n')

    numpages = get_pdfinfo(filename)

    for line in text:
        if not line and clean_text[-2:] != '\n\n':
            clean_text += '\n'
        else:
            if "disclosure" in line.lower(): break
            for exc_line in exclude_sent_with_words:
                if re.search(r'\b' + exc_line.lower() + r'\b', line.lower()): break
            else:
                if text.count(line) <= max(numpages-10, 4):
                    #remove extra spaces
                    clean_line = re.sub(r'\s+', ' ', line)
                    clean_line = utils.remove_nonsense_lines(str(clean_line), 6)
                    if clean_line:
                        clean_text += clean_line + '\n'
    return clean_text
    

def read_txt_file(fname):

    with open(fname) as f:
        content = f.readlines()

    # remove whitespace characters like `\n` at the end of each line
    content = [x.strip().lower() for x in content]

    return content


def text_summary(text, numlines=7, lang='english'):

    # Preprocessing
    text = re.sub('\s+', ' ', text)

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
        if len(sent.split()) <= 30 and len(sent.split()) >= 4:
            # find all words in the sentence
            words = tokenizer.tokenize(sent)
            words = [stemmer.stem(w.lower()) for w in words if w.isalpha() and not w.isdigit()]

            # sum of term frequencies
            for word in words:
                sentence_scores[sent] += tokens_frequencies.get(word, 0)

    summary_sentences = heapq.nlargest(numlines, sentence_scores, key=sentence_scores.get)

    summary_sentences = [ sent.strip().capitalize() for sent in summary_sentences]
    
    summary = '\n'.join(summary_sentences)

    return summary


if __name__ == '__main__':

    numlines = 5
    exclude_sent_with_words = read_txt_file('./exclude_words.txt')
    
    url = 'https://en.wikipedia.org/wiki/Spain'
    filename = '/home/laptop/eclipse-workspace/babieca/pms/backup/repository/files/equity/GUANGZHOU_20190104_0000.pdf'
    
    #text = fetch_url(url)
    text = read_pdf_file(filename, exclude_sent_with_words)
    
    summary = text_summary(text, numlines)

    print(summary)
    #print('\n'.join(textwrap.wrap(summary, 80, break_long_words=False)))

    sys.exit(0)



