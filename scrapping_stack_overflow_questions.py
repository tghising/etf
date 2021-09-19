import requests
from bs4 import BeautifulSoup
import pandas as pd

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'}

questionList = []

def getQuestions(tag, page):
    url = f'https://stackoverflow.com/questions/tagged/{tag}?tab=Active&page={page}&pagesize=50'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    questions = soup.find_all('div', {'class': 'question-summary'})
    print("**************************   *********************")
    print(questions)
    for item in questions:
        question = {
        'tag': tag,
        'title': item.find('a', {'class': 'question-hyperlink'}).text,
        'link': 'https://stackoverflow.com' + item.find('a', {'class': 'question-hyperlink'})['href'],
        'votes': int(item.find('span', {'class': 'vote-count-post'}).text),
        'date': item.find('span', {'class': 'relativetime'})['title'],
        }
        questionList.append(question)
    return

for x in range(1,3):
    getQuestions('python', x)
    getQuestions('flask', x)

df = pd.DataFrame(questionList)
df.to_excel('stackquestions.xlsx', index=False)
print('Fin.')