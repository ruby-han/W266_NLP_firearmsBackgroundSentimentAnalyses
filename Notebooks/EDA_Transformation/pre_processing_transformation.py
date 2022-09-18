import multiprocessing

import pandas as pd
import re
from profanity_filter import ProfanityFilter
pf = ProfanityFilter()
train = pd.read_csv('train.csv').set_index('id')
test = pd.read_csv('test.csv').set_index('id')

def comment_transforms(comment_string, phase = 4):
  if phase == 1:
      comment_string = str.lower(comment_string)
      comment_string = comment_string.replace('{','')\
                                      .replace('}','')\
                                      .replace('>','')\
                                      .replace('<','')\
                                      .replace('?','.')\
                                      .replace('-',' ')\
                                      .replace('...',' ')\
                                      .replace('\n',' ')\
                                      .replace(':','')\
                                      .replace('"','')\
                                      .replace('!','')
  elif phase == 2:   
      comment_string = str.lower(comment_string)
      comment_string = comment_string.replace('{','')\
                                      .replace('}','')\
                                      .replace('>','')\
                                      .replace('<','')\
                                      .replace('?','.')\
                                      .replace('-',' ')\
                                      .replace('...',' ')\
                                      .replace('\n',' ')\
                                      .replace(':','')\
                                      .replace('"','')\
                                      .replace('[','')\
                                      .replace(']','')\
                                      .replace('*','')\
                                      .replace('\t',' ')\
                                      .replace('\\','')\
                                      .replace('\/','')\
                                      .replace('=','')\
                                      .replace('@','')\
                                      .replace('(','')\
                                      .replace(')','')\
                                      .replace('|','')\
                                
      # remove sensitive info such as IP, username
      comment_string = re.sub('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',' ', comment_string)
      comment_string = re.sub('\[\[.*\]','', comment_string)
      comment_string = re.sub(' +', ' ', comment_string)
      comment_string = re.sub('-+', ' ', comment_string)
      comment_string = re.sub('\.+', '.', comment_string)
      comment_string = re.sub('\^+', ' ', comment_string)
  
  elif phase == 3:  
      comment_string = comment_string.replace('{','')\
                                      .replace('}','')\
                                      .replace('>','')\
                                      .replace('<','')\
                                      .replace('-',' ')\
                                      .replace('...',' ')\
                                      .replace('\n',' ')\
                                      .replace(':','')\
                                      .replace('"','')\
                                      .replace('[','')\
                                      .replace(']','')\
                                      .replace('*','')\
                                      .replace('\t',' ')\
                                      .replace('\\','')\
                                      .replace('\/','')\
                                      .replace('=','')\
                                      .replace('@','')\
                                      .replace('(','')\
                                      .replace(')','')\
                                      .replace('|','')\
                                
      # remove sensitive info such as IP, username
      comment_string = re.sub('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',' ', comment_string)
      comment_string = re.sub('\[\[.*\]','', comment_string)
      comment_string = re.sub(' +', ' ', comment_string)
      comment_string = re.sub('-+', ' ', comment_string)
      comment_string = re.sub('\.+', '.', comment_string)
      comment_string = re.sub('\^+', ' ', comment_string)
    
  elif phase == 4:
      
      comment_string = comment_string.replace('{','')\
                                      .replace('}','')\
                                      .replace('>','')\
                                      .replace('<','')\
                                      .replace('-',' ')\
                                      .replace('...',' ')\
                                      .replace('\n',' ')\
                                      .replace(':','')\
                                      .replace('"','')\
                                      .replace('[','')\
                                      .replace(']','')\
                                      .replace('\t',' ')\
                                      .replace('\\','')\
                                      .replace('\/','')\
                                      .replace('=','')\
                                      .replace('@','')\
                                      .replace('(','')\
                                      .replace(')','')\
                                      .replace('|','')
                                
      # remove sensitive info such as IP, username
      comment_string = re.sub('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',' ', comment_string)
      comment_string = re.sub('\[\[.*\]','', comment_string)
      comment_string = re.sub(' +', ' ', comment_string)
      comment_string = re.sub('-+', ' ', comment_string)
      comment_string = re.sub('\.+', '.', comment_string)
      comment_string = re.sub('\^+', ' ', comment_string)
      comment_string = pf.censor(text=comment_string)
  
  return comment_string


def worker(procnum,df, return_dict):
    """worker function"""
    print(str(procnum) + " represent!")
    return_dict[procnum] = df['comment_text'].apply(comment_transforms)


if __name__ == "__main__":
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    jobs = []
    for i in range(30):
        print(i)
        start_range = i*5319
        end_range = (i+1)*5319
        if i==29:
            end_range = 159571
        print(f"start_range {start_range}, end_range {end_range}")

    new_df = None

    print("Finished train dataset")
    print("Starting test dataset")
    return_dict = manager.dict()
    jobs = []
    for i in range(30):
        print(i)
        start_range = i*2132
        end_range = (i+1)*2132
        if i==29:
            end_range = 63978
        print(f"start_range {start_range}, end_range {end_range}")
        p = multiprocessing.Process(target=worker, args=(i, test[start_range:end_range], return_dict))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()
    new_df = None      
    for key in sorted(return_dict.keys()):
        new_df = pd.concat([new_df,return_dict[key]])
    new_df.to_csv('test_profanity_censor.csv', index=False)
