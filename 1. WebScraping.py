#!/usr/bin/env python
# coding: utf-8

# In[8]:


import requests
from bs4 import BeautifulSoup
from bs4 import Comment
import pandas as pd
import re
import time


def headline_extraction(soup):
    headline = soup.find('title')
    headline = str(headline).replace('<title>','')
    headline = str(headline).replace('</title>','')
    return headline
  
def id_and_lang_extraction(soup):
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    line = str(comments).split()
    try:
        id_number = line[2]
        id_number= str(id_number).replace(',','')
        language = line[4]
    except:
        id_number = ["","","","",""]
        language = ["","","","",""]
    return(id_number, language)
    
def date_and_time_extraction(soup):
    string = soup.findAll(text=re.compile('Published:\r\n                        20'))
    if not string:
        string = soup.findAll(text=re.compile('Publicerad:\r\n                        20'))

    if not string:
        string = soup.findAll(text=re.compile('Publicerad:\r\n                        19'))

    if not string:
        string = soup.findAll(text=re.compile('Published:\r\n                        19'))

    string = str(string).split()
    date_time = string[2]+" " + string[3]
    return date_time

def text_extraction(soup):
    try:
        txt_result = soup.find(class_='txtPre')
        text = txt_result.get_text(strip=True, separator = " ")
    except:
        txt_result = soup.find('table', id='previewTable')
        text = txt_result.get_text(strip=True, separator = " ")
    text = str(text).replace("\xa0", " ")
    text = str(text).replace("\n", " ")
    text = str(text).replace('\r'," ")
    return text

def save_scraped_documents(file_number, document_id, document_language, 
                    document_date_time, document_headline, document_text):
    
    path = 'export'+str(file_number)+'.json'
    document_array = [document_id,document_language,document_date_time,document_headline,document_text]
    df = pd.DataFrame(document_array).transpose()
    df.columns = ['doc_id','language','date_time','headline','text']
    df.to_json(path_or_buf=path)
    print('Saved scraped documents as '+path)
    

def input_parameters():
    start_id = int(input('Enter start document ID: '))
    stop_id = int(input('Enter stop document ID: '))
    save_frequency = int(input(
        '''How frequently would you like to save the scraped documents? 
    Specify a number, such as "500", if you want to save a file every 500 documents.
    The export format is .json and the destination folder is your current directory.'''))
    
    first_language = input('''
        Specify the language of the documents you want to scrape,
        either "sv" (Swedish) or "en" (English). Other languages do currently not work''')
    
    question = '''Would you like to scrape documents of another language?
            Since many companies publish press releases in multiple languages, 
            multiple choices might result in duplicates: '''
    reply = yes_or_no(question)
    
    if reply == True:
        second_language = input('''
        Specify the language of the documents you want to scrape,
        either "sv" (Swedish) or "en" (English). Other languages do currently not work''')
        languages = (first_language+' '+second_language).split()
    elif reply == False:
        languages = first_language.split()
        pass

    return start_id, stop_id, save_frequency, languages
    
def yes_or_no(question):
    
    reply = input(question+' (y/n): ').lower().strip()
    if reply[0] == 'y':
        return True
    elif reply[0] == 'n':
        return False
    else:
        return yes_or_no("Please answer either yes or no")
    
    
def main(start_id, stop_id, save_frequency, languages):
    
    print('Start id: ', start_id)
    print('Stop id: ', stop_id)
    print('Saving frequency: Every', save_frequency, 'documents')
    print('Languages: ', languages)
    
    document_id = []
    document_date_time = []
    document_language = []
    document_headline = []
    document_text = []

    already_saved=1
    file_number=0
    
    current_id = int(start_id)
    scraped_documents_counter=0
    print('No. of docs scraped in this session; Doc ID')
    
    while current_id > stop_id:
        URL = 'https://newsclient.omxgroup.com/cdsPublic/viewDisclosure.action?disclosureId='+str(current_id)
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, 'html.parser')
        id_and_lang=id_and_lang_extraction(soup)
        
        if id_and_lang[1] in languages:

            date_and_time=date_and_time_extraction(soup)
            document_id.append(id_and_lang[0])
            document_language.append(id_and_lang[1])
            document_date_time.append(date_and_time)
            document_headline.append(headline_extraction(soup))
            document_text.append(text_extraction(soup))
            
            scraped_documents_counter+=1
            already_saved=0
            print(scraped_documents_counter, current_id)
            
            #Delay each successful scrape by 0.1 seconds. 
            #If the scrape is conducted faster, Nasdaq interrupts it.
            #Could be a DoS defense mechanism or equivalent.
            time.sleep(0.1)
        else:
            pass  

        #if the loop has saved y documents; append them to the df and save the whole df to a new .json file
        if scraped_documents_counter%save_frequency==0 and already_saved==0:
            save_scraped_documents(file_number, document_id, document_language, 
                                    document_date_time, document_headline, document_text)
            file_number+=1
            already_saved=1
        else:
            pass
        current_id-=1

    save_scraped_documents(file_number, document_id, document_language, 
                            document_date_time, document_headline, document_text)
    
    print("The script has stopped since the stop ID was reached. Please restart to continue, defining new start and stop ID's.")
    
if __name__ == '__main__':
    #Start ID in this study = 926602. Stop ID is 226500. Language is Swedish.
    start_id, stop_id, save_frequency, languages = input_parameters()
    
    main(start_id, stop_id, save_frequency, languages)

