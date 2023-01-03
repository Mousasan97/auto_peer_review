import run_models

import sys
from copy import deepcopy
import pandas as pd
import mysql
from mysql.connector import MySQLConnection, Error
from langdetect import detect
import regex as re
import pickle

import json

dont_split = ['main_title','other_title','main_abstract','other_abstract','main_keywords','other_keywords']
dict_ = {k: [] for k in ['main_title','main_authors','main_affiliation','main_abstract','main_keywords',
                          'other_title','other_authors','other_affiliation','other_abstract','other_keywords',
                          'bib_info','dates','correspondence','references','wrongly_labeled']}

margin_headings = ['الهوامش','هوامش البحث','هوامش وتعليقات البحث','الحواشي والتعليقات','هوامش الدراسة','الهوامش والاشارات','الإحالات','الاحالات','الهوامش والإحالات','الهوامش والاحالات']
reference_headings = ['المصادر','مصادر البحث','الإحالات والمراجع','المراجع','الهوامش والمراجع','مراجع البحث','المراجع والهوامش','الهوامش والمراجع المعتمدة','الهوامش و المراجع المعتمدة','المصادر والمراجع','ثبت المصادر والمراجع','المراجع والاحالات','الهوامش والمصادر','المصادر والمراجع المعتمدة','قائمة المصادر والمراجع','قائمة المراجع',"قائمة المراجع",'مراجع وهوامش البحث','قائمة المصادر','قائمة المصادر','مراجع وهوامش البحث','مكتبة البحث','الاحالات والمراجع','References','المصادر References','الإحالات','المصادر والإحالات المعتمدة','الببليوغرافيا','المراجع References','قائمة المراجع والمصادر' ,'قائمة ببليوجرافية','الإحالات والهوامش','الاحالات والهوامش','قائمة المصادروالمراجع','المراجع العربية','قائمة المراجع باللغة العربية','قائمة المراجع باللغة الإنجليزية','قائمة المراجع باللغة الأجنبية','المراجع الأجنبية','المراجع الأجنبية','المراجع الإنجليزية','المراجع باللّغة العربية','المراجع باللّغة الإنجليزية','مراجع باللغة العربية','مراجع باللغة الإنجليزية','المراجع الاجنبية',"أولاً: المراجع العربية","أولا: المراجع العربية","ثانياً: المراجع الأجنبية",'ثانيا: المراجع الأجنبية','المراجع الانجليزية','المراجع باللغة العربية','المراجع باللغة الأجنبية']


ref_margin = ['الهوامش والمراجع','الهوامش والمراجع المعتمدة','المراجع والهوامش','الهوامش والمصادر','مراجع وهوامش البحث']
intros = ['المقدمة','مقدمة','تقدمة','المقدمة وأهمية البحث','Introduction','المقدمة وأهمية البحثIntroduction','تمهيد',"مقدمه","مقدمة الدراسة",'تقديم','المقدمة Introduction','المقدمة Introduction']
imprts = ['أهمية البحث','أهمية الدراسة','المقدمة وأهمية البحث','المقدمة وأهمية الدراسة','اهمية الدراسة']
probs = ['مشكلة البحث','مشكلة الدراسة','إشكالية الدراسة','إشكالية البحث','اشكالية البحث','اشكالية الدراسة','تساؤلات البحث','تساؤلات الدراسة','أسئلة الدراسة','أسئلة البحث','مشكلات الدراسة']
goals = ['أهداف الدراسة','أهداف البحث','هدف الدراسة','هدف البحث','هدفا البحث','هدفا الدراسة','اهداف الدراسة']
assmps = ['فرضية البحث','فرضية الدراسة','فرضيات البحث','فرضيات الدراسة','أسئلة الدراسة وفرضياتها']
methods = ['منهج البحث','منهج الدراسة','منهجية الدراسة','منهجية البحث','الإطار المنهجي','الإطار المنهجي للبحث','الإطار المنهجي للدراسة','نهج الدراسة و أدواتها']
sample_pop = ['مجتمع الدراسة','مجتمع البحث','مجتمع وعينة الدراسة','أفراد الدراسة']
sample_size = ['عينة الدراسة','عينة البحث','مجتمع وعينة الدراسة']
data_collection = ['أداة البحث','أداة الدراسة','طريقة جمع العينة','طرق جمع العينات','منهج الدراسة و أدواتها',"أساليب جمع البيانات","اساليب جمع البيانات"]
lits = ['الدراسات السابقة','الدراسات السابقة والمشابهة:']
timelines = ['الإطار الزمني','الإطار الزمني للبحث','الإطار الزمني للدراسة','الإطار النظري','حدود الدراسة','حدود البحث','الحدود الزمنية','مجال الدراسة و حدودها','الإطار العام للدراسة','الاطار العام للدراسة']
results = ['نتائج البحث ومناقشتها','النتائج','نتائج الدراسة','نتائج البحث','الاستنتاجات','نتائج الدراسة ومناقشتها','النتائج العامة للدراسة','النتائج Results']
recs = ['التوصيات','المقترحات','التوصيات والمقترحات','توصيات الدراسة','توصيات','الاستنتاجات والمقترحات']
conclusions = ['الخاتمة','خاتمة','الاستنتاجات','الاستنتاج  Conclusion','الاستنتاج','Conclusion','الاستنتاج Conclusion']

email_services = ['@gmail', '@hotmail', '@aol', '@yahoo', '@qq.com','@msn']

with open('data/list_of_countries.txt', "rb") as f:
    countries = pickle.load(f)

with open('data/list_of_cities.txt', "rb") as f:
    cities = pickle.load(f)
    
def insert_fields(variables):
    query = "INSERT INTO ai_document_details(doc_id,field,is_exist,field_text,field_count) " \
            "VALUES(%s,%s,%s,%s,%s)"

#     args = (doc_id, field,is_exist,field_text,field_count)
    conn = None
    try:
        with mysql.connector.connect(host='127.0.0.1',
                                   database='emarefa',
                                   user='yasmeen',
                                   password='M@ref@2022') as connection:

            with connection.cursor() as cursor:
                for variable in variables:
                    try:
                        cursor.execute(query, variable)
                    except Error as error:
                        print(error)
                        pass

                if cursor.lastrowid:
                    print('last insert id', cursor.lastrowid)
                else:
                    print('last insert id not found')

            connection.commit()

    except Error as error:
        print(error)
        
def apa_style(citation,all_text,cities,countries):
    YEAR_apa = r"\b\d{4}"
    
    type_ = 'unknown'
    error = 0
    not_in_text = 0
    ar_num = '۰١٢٣٤٥٦٧٨٩'
    en_num = '0123456789'
    table = str.maketrans(ar_num,en_num)
    citation = citation.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').strip(':|.| |-|–')
    try:
        before_year = re.compile('.*\.\s*\(\s*'+YEAR_apa+'\s*[مه]?\s*\)\s*\.').findall(citation)[0]
    except:
        error = 1
        try:
            before_year = re.compile('.*[\.\،\,]?\s*\(\s*'+YEAR_apa+'\s*[مه]?\s*\)\s*[\.\:\,]?').findall(citation)[0]
#             print('author and year not seperated by .')
        except:
            try:
                before_year = re.compile('.*[\.\،\,]\s*\(?\s*'+YEAR_apa+'\s*[مه]?\s*\)?\s*[\.\:\,]').findall(citation)[0]
#                 print('Year not between brackets')
            except:
#                 print('author and year formatting is wrong')
                error = 1
                before_year = ''
                pass
    if before_year:
        if len(re.split('\s\w?\.?\w\.',before_year)) >0:
            try:
                year = re.compile('.*[\.\،\,]?\s*\(\s?('+YEAR_apa+')\s?[مه]?\s*\)\s*[\.\:\,]').findall(citation)[0]
                year = year.translate(table)
            except:
#                 print('Year and title not seperated by .')
                try:
                    year = re.compile('.*[\.\،\,]?\s*\(\s?('+YEAR_apa+')\s?[مه]?\s*\)\s*[\.\:\,]?').findall(citation)[0]
                    year = year.translate(table)
                    error = 1
                except:
#                     print('no year')
                    error = 1
                    not_in_text = 1
                    return not_in_text,error
#             print(year)
            authors = re.compile('(.*[\.\،\,]?\s*)\('+YEAR_apa+'\s*[مه]?\s*\)\s*[\.\:\,]?').findall(citation)[0]
            authors = re.split('\.\,|\.\،|&|and',authors)
#             print('authors: {}'.format(authors))
            if len(authors) == 1 and len(re.split(",|،",authors[0])) > 2 and len(authors[0].split(' و'))>1:
                authors = authors[0].split(' و')
               
            if authors:
                authors = [re.split("،|,",x) for x in authors if x.strip() != '']  
                last_names = [x[0].strip() for x in authors]
#                 print(last_names)
                if len(last_names) == 1:
                    in_text_citation = re.compile('\(?\s*'+last_names[0]+'\s*[\,\،]?\s*'+'\(?\s?'+year)
                    in_text_citation1 = ''
                    in_text_citation2 = ''
                if len(last_names) == 2:
                    in_text_citation = re.compile('\(?\s*'+last_names[0]+'\s*(&|and|و)\s?'+last_names[1]+'[\,\،]?\s*'+'\(?\s?'+year)
                    in_text_citation1 = ''
                    in_text_citation2 = ''
                elif len(last_names) >2:
    #                 if last_names[-1] == 'et al.' or last_names[-1] == 'et al' or last_names[-1] == 'وآخرون' or last_names[-1] == 'آخرون':
                    in_text_citation = re.compile('\(?\s*'+last_names[0]+'\s+(&? ?et al\.?|و[اآ]خرون)[\,\،]?\s*'+'\(?\s?'+year)
                    list_last_names1 = ", ".join(last_names[:-1])
                    list_last_names2 = "، ".join(last_names[:-1])
                    in_text_citation1 = re.compile('\(?\s*'+ list_last_names1+'\s*[\,\،]?\s+(&|and|و)\s*'+last_names[-1]+',?\s*'+'\(?\s?'+year)
                    in_text_citation2 = re.compile('\(?\s*'+ list_last_names2+'\s*[\,\،]?\s+(&|and|و)\s*'+last_names[-1]+',?\s*'+'\(?\s?'+year)

                try:
                    re.search(in_text_citation,all_text).group(0)
                    not_in_text = 0
                except:
                    if in_text_citation1:
                        try:
                            re.search(in_text_citation1,all_text).group(0)
                            not_in_text = 0
                        except:
                            try:
                                re.search(in_text_citation2,all_text).group(0)
                                not_in_text = 0
                            except:
                                not_in_text = 1
                    else:
                        not_in_text = 1
                    
            else:
#                 print("There is an error in authors' names")
                error = 1
                in_text_citation = ''
                not_in_text = 1
    else:
#         print("There is an error in authors' names")
        error = 1
        in_text_citation = ''
        not_in_text = 1
            
    try:
        is_thesis = re.compile(r"\(*[اأ]طروح[ةه]|[Tt]hesis|[Tt]heses|[Dd]issertations?|رسالة ماجستير|رسالة دكتوراه|رسالة دكتوراة\)").findall(citation)[0]
        type_ = 'thesis'
        try:
            title = re.compile(r"[\.\،\,]?\s*\(?"+YEAR_apa+"\s*[مه]?\s*\)?\s*[\.\:\,]?\s*(\w+[^\.\,]*)\.\s*\w+[^\.\,\d]+").findall(citation)[0]
            type_ = 'thesis'
        except:
#             print('no title')
            error = 1
    except:
#         print('not thesis')
        
        try:
            book_country = re.compile(r"(\w+[^\,]*\,\s*\w+\:)").findall(citation)[0]
            type_ = 'book'
        except:
            try:
                pub_country = re.compile(r"[\.\,\،]\s*\L<cities>[\.\,\،]?\s*\L<countries>[\:\,\،]", cities=cities,countries=countries).findall(citation)[0]
                error = 1
#                 print('country and publisher not seperated by :')
                type_ = 'book'
            except:
                try:
                    book_ed = re.compile(r"[\.\,\،]\s*(ط\s?\d|ا?ل?طبعة|[Ee][Dd][Ii]?[Tt]?[Ii]?[Oo]?[Nn]?)\s*\d?\s*").findall(citation)[0]
                    type_ = 'book'
                except:
#                     print('not book')
                    try:
                        vol_pages = re.compile(r"\d+\s*\(?\d*\)?\s*[\,\،]\s*\d{,4}-\d{,4}").findall(citation)[0]
                        type_ = 'article'
                    except:
    #                     print('are volume and issue present?')
                        error = 1
                        try:
                            pages = re.compile(r"\s*\d{,4}\s?-\s?\d{,4}").findall(citation)[0]
    #                         print('only pages can be found without volume\issue')
                            error = 1
                            type_ = 'article'
                        except:
                            try:
                                vol_issue = re.compile(r"([Vv][Oo][Ll]|مجل?د?|[Ii][Ss][Ss][Uu][Ee]|ا?ل?عدد)?\s*\(?\s*\d+\s*\)?[\,\،]\s*").findall(citation)[0]
    #                             print('only volume\issue found')
                                error = 1
                                type_ = 'article'
                            except:
    #                             print('are volume, issue and article pages present?')
                                try:
                                    pub_country = re.compile(r"[\.\,\،]\s*\L<cities>?[\.\,\،]?\s*\L<countries>?[\:\,\،]?", cities=cities,countries=countries).findall(citation)[0]
                                    error = 1
                                    type_ = 'book'
                                except:
                                    error = 1
                                    pass 

                    try:
                        title = re.compile(r"[\.\،\,]?\s*\(?\s*"+YEAR_apa+"\s*[مه]?\s*\)?\s*[\.\:\,]?\s*(\w+[^\.]*)\.\s*\w+[^\.\,\d\،]+").findall(citation)[0]
    #                     type_ = 'article'
                    except:
                        try:
                            title = re.compile(r"[\.\،\,]?\s*\(?\s*"+YEAR_apa+"\s*[مه]?\s*\)?\s*[\.\:\,]?\s*(\w+[^\.\,\،]*)[\.\,\،]\s*\w+[^\.\,\d\،]+").findall(citation)[0]
                            print('Are the title and journal name not seperated by .')
                            error = 1
    #                         type_ = 'article'
                        except:
                            print('are title and journal name present?')
                            error = 1
                        
    return not_in_text,error,type_

if __name__ == "__main__":
    file_path = sys.argv[1]
#     check_ref = sys.argv[2]
#     if check_ref = 'apa':
#         check_apa = True
#         check_ama = False
#         check_mla = False
#     elif check_ref = 'ama':
#         check_apa = False
#         check_ama = True
#         check_mla = False
#     elif check_ref = 'mla':
#         check_apa = False
#         check_ama = False
#         check_mla = True
#     else:
#         check_apa = False
#         check_ama = False
#         check_mla = False
    
    RN = run_models.Structure('models/first_model.sav','models/metadata_model.sav',
                                 'scalers/first_model_scaler.sav','scalers/metadata_model_scaler.sav',
                                 file_path,'data/full_dataset7.csv')
    try:
        meta,ref,heading,all_text = RN.fit_models()
    except Exception as e:
        print(e);

    meta = meta[meta['level_0'].str.strip() != '']
    dictt = deepcopy(dict_)

    if len(file_path.split('/')) > 1:

        file_name = file_path.split('/')[-1]
    else:
        file_name = file_path

    prev_label = ''
    prev_index = 0
    for i in range(len(meta)):
        if (meta.iloc[i]['sub_label'].strip() in dont_split) and (i != 0) and (len(dictt[meta.iloc[i]['sub_label'].strip()]) !=0) and (meta.iloc[i]['sub_label'].strip() != prev_label or (prev_index +1 != meta.iloc[i].name)):
            dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = 'wrongly_labeled'
            prev_index = meta.iloc[i].name
            continue
        elif (meta.iloc[i]['sub_label'].strip() == 'main_authors') and (len(dictt['main_title']) ==0) and i <3:
            dictt['main_title'].append(meta.iloc[i]['level_0'])
            prev_label = 'main_title'
            prev_index = meta.iloc[i].name
        elif (meta.iloc[i]['sub_label'].strip() == 'main_abstract') and meta.iloc[i]['level_0'].strip(':|.| |-') in intros:
            dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = 'wrongly_labeled'
            prev_index = meta.iloc[i].name
            continue
        elif (meta.iloc[i]['sub_label'].strip() == 'main_keywords') and (len(dictt['main_abstract']) >=0) and (prev_index +1 != meta.iloc[i].name):
            dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = 'wrongly_labeled'
            prev_index = meta.iloc[i].name
        elif (meta.iloc[i]['sub_label'].strip() == 'other_keywords') and (len(dictt['other_abstract']) <1) and 'other_abstract' in meta['sub_label'].to_list():
            dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = 'wrongly_labeled'
            prev_index = meta.iloc[i].name
            continue
        elif (meta.iloc[i]['sub_label'].strip() == 'other_abstract') and 'other_title' in meta['sub_label'].to_list() and len(dictt['other_title']) <1:
            dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = 'wrongly_labeled'
            prev_index = meta.iloc[i].name
            continue
        elif (meta.iloc[i]['sub_label'].strip() == 'other_title') and (len(dictt['other_abstract']) >0):
            dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = 'wrongly_labeled'
            prev_index = meta.iloc[i].name
            continue
        elif (meta.iloc[i]['sub_label'].strip() == 'other_keywords') and len(dictt[meta.iloc[i]['sub_label'].strip()]) > 0 and prev_label != 'other_keywords':
    #             dictt['wrongly_labeled'].append(meta.iloc[i]['level_0'])
            prev_label = meta.iloc[i]['sub_label'].strip()
            prev_index = meta.iloc[i].name
            continue
        else:
            dictt[meta.iloc[i]['sub_label'].strip()].append(meta.iloc[i]['level_0'])
            prev_label = meta.iloc[i]['sub_label'].strip()
            prev_index = meta.iloc[i].name

#     dictt['file_name'].append(file_name)
   # dictt['headings'].append(heading['level_0'].to_list())

    dictt['references'].append(ref['level_0'].to_list())
    if len(dictt['references']) >0:
        dictt['references'] = dictt['references'][0]
#     all_results_df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in dictt.items() ]))

    variables = []
    doc_id = file_name.split(".docx")[0]
#    doc_id = '04'

    body_headings = heading['level_0'].to_list()
    intro = [s for s in intros if any(s in xs for xs in body_headings)]
#     intro = [x for x in body_headings if x.strip(':|.| |-') in intros]
    if len(intro) > 0:
        variables.append([doc_id,'intro_heading','YES',intro[0],0])
    else:
        variables.append([doc_id,'intro_heading','NO','',0])

    imprt = [s for s in imprts if any(s in xs for xs in body_headings)]
    if len(imprt) > 0:
        variables.append([doc_id,'imprt_heading','YES',imprt[0],0])
    else:
        variables.append([doc_id,'imprt_heading','NO','',0])

    prob = [s for s in probs if any(s in xs for xs in body_headings)]
    if len(prob) > 0:
        variables.append([doc_id,'prob_heading','YES',prob[0],0])
    else:
        variables.append([doc_id,'prob_heading','NO','',0])

    goal = [s for s in goals if any(s in xs for xs in body_headings)]
    if len(goal) > 0:
        variables.append([doc_id,'goal_heading','YES',goal[0],0])
    else:
        variables.append([doc_id,'goal_heading','NO','',0])

    assmp = [s for s in assmps if any(s in xs for xs in body_headings)]
    if len(assmp) > 0:
        variables.append([doc_id,'assmp_heading','YES',assmp[0],0])
    else:
        variables.append([doc_id,'assmp_heading','NO','',0])

    method = [s for s in methods if any(s in xs for xs in body_headings)]
    if len(method) > 0:
        variables.append([doc_id,'method_heading','YES',method[0],0])
    else:
        variables.append([doc_id,'method_heading','NO','',0])

    s_pop = [s for s in sample_pop if any(s in xs for xs in body_headings)]
    if len(s_pop) > 0:
        variables.append([doc_id,'sample_pop_heading','YES',s_pop[0],0])
    else:
        variables.append([doc_id,'sample_pop_heading','NO','',0])

    s_size = [s for s in sample_size if any(s in xs for xs in body_headings)]
    if len(s_size) > 0:
        variables.append([doc_id,'sample_size_heading','YES',s_size[0],0])
    else:
        variables.append([doc_id,'sample_size_heading','NO','',0])

    data_coll = [s for s in data_collection if any(s in xs for xs in body_headings)]
    if len(data_coll) > 0:
        variables.append([doc_id,'data_collection_heading','YES',data_coll[0],0])
    else:
        variables.append([doc_id,'data_collection_heading','NO','',0])

    lit = [x for x in lits if x in body_headings]
    if len(lit) > 0:
        variables.append([doc_id,'lit_heading','YES',lit[0],0])
    else:
        variables.append([doc_id,'lit_heading','NO','',0])

    timeline = [s for s in timelines if any(s in xs for xs in body_headings)]
    if len(timeline) > 0:
        variables.append([doc_id,'timeline_heading','YES',timeline[0],0])
    else:
        variables.append([doc_id,'timeline_heading','NO','',0])

    result = [s for s in results if any(s in xs for xs in body_headings)]
    if len(result) > 0:
        variables.append([doc_id,'result_heading','YES',result[0],0])
    else:
        variables.append([doc_id,'result_heading','NO','',0])

    rec = [s for s in recs if any(s in xs for xs in body_headings)]
    if len(rec) > 0:
        variables.append([doc_id,'recs_heading','YES',rec[0],0])
    else:
        variables.append([doc_id,'recs_heading','NO','',0])
        
    conclusion = [s for s in conclusions if any(s in xs for xs in body_headings)]
    if len(conclusion) > 0:
        variables.append([doc_id,'conclusion_heading','YES',conclusion[0],0])
    else:
        variables.append([doc_id,'conclusion_heading','NO','',0])

    for k,v in dictt.items():
        field = k
        pat_main_kywrd = re.compile('(كلمات|الكلمات)\s*(المفتاحيّة|المفتاحيّه|الداله|الاصطلاحية|المفتاحيه|المفاتيح|الدالّة|مفتاحية|مفتاحيّة|المفتاح|الافتتاحية|مفتاحيه|المفتاحية|الدالة)')
        pat_other_kywrd = re.compile('[Kk]ey\s?[Ww]ords?\s*:?|Keys words\s*:?|Mots\s?-?\s?[Cc]lés\s?:?')
        pat_aff = re.compile(r'معهد|[Un][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy]|جامعة|الجامعة|قسم|كلية|[Dd][Ee][Pp][Aa]?[Rr]?[Tt]?[Mm]?[Ee]?[Nn]?[Tt]?|College|الجامعي')
        
        if len(v) > 0 and k != 'wrongly_labeled':
            is_exist = "YES"
            
            if k == 'main_authors':
                continue
            if k == 'main_affiliation' and (len(v) > len(dictt['main_authors'])):
                field_text = v[:len(dictt['main_authors'])]
                field_text = ' '.join([str(i) for i in field_text])
                
            if k == 'other_affiliation' and (len(v) > len(dictt['other_authors'])):
                field_text = v[:len(dictt['other_authors'])]
                field_text = ' '.join([str(i) for i in field_text]) 
                
            if k == 'references':
                #field_text = [x for x in dictt['references'] if x.strip(':|.| |-') in reference_headings]
                field_text = ''
            else:
                field_text = ' '.join([str(i) for i in v])

            if k == 'main_abstract' or k == 'other_abstract':
                field_count = len(field_text.split( ))
            else:
                field_count = 0

            if k == 'correspondence':
                not_institutional = any(email_service in field_text.strip(':|.| |-') for email_service in email_services)
                if not_institutional == 'True':
                    variables.append([doc_id, 'not_institutional_email','NO','',0])
                else:
                    variables.append([doc_id, 'not_institutional_email','YES',field_text,0])
                    
        elif len(v) <= 0 and  k != 'wrongly_labeled':
            if k == 'main_title':
                is_exist = "YES"
                field_text = ''
                
            if k == 'main_authors':
                continue

            elif k == 'main_affiliation':
                
                aff = [x for x in dictt['wrongly_labeled'] if len(pat_aff.findall(x)) >0]
                if len(aff) > 0:
                    is_exist = "YES"
                    field_text = aff[0]
                else:
                    is_exist = "NO"
                    field_text = ''

            elif  k == 'main_keywords':
                main_kywrds = [x for x in dictt['wrongly_labeled'] if len(pat_main_kywrd.findall(x)) >0]
                ## main and other keywords are joined
                if len(main_kywrds) > 0 or pat_main_kywrd.findall(' '.join([str(i) for i in dictt['other_keywords']])):
                    is_exit = 'YES'
                    if pat_main_kywrd.findall(' '.join([str(i) for i in dictt['other_keywords']])):
                        field_text = ' '.join([str(i) for i in dictt['other_keywords']])
                    elif len(main_kywrds) > 0:
                        field_text = main_kywrds[0]
                else:
                    is_exist = "NO"
                    field_text = ''
                    
            elif  k == 'other_keywords':
                other_kywrds = [x for x in dictt['wrongly_labeled'] if len(pat_other_kywrd.findall(x)) >0]
                if len(other_kywrds) > 0:
                    is_exit = 'YES'
                    field_text = other_kywrds[0]
                else:
                    is_exist = "NO"
                    field_text = ''
                    
            elif k == 'main_abstract':
                if len(dictt['other_abstract']) > 0:
                    is_exist = 'MAYBE'
                    field_text = ''
                else:
                    is_exist = 'NO'
                    field_text = ''

            elif k == 'correspondence':
                pat_email = re.compile('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+')
                mails = []
                try:
                    tt = ' '.join([str(i) for i in dictt['main_affiliation']])
                    if len(pat_email.findall(tt)) > 0:
                        mails.append(pat_email.findall(tt)[0])
                    tt2 = ' '.join([str(i) for i in dictt['other_affiliation']])
                    if len(pat_email.findall(tt2)) > 0:
                        mails.append(pat_email.findall(tt2)[0])
                except:
                    try:
                        mails.append([x for x in dictt['wrongly_labeled'] if len(pat_email.findall(x)) >0])
                    except:
                        pass

                if len(mails)>0:
                    is_exist = "YES"
                    field_text = ' '.join([str(i) for i in mails])
                    not_institutional = any(email_service in field_text for email_service in email_services)
                    if not_institutional == 'True':
                        variables.append([doc_id, 'not_institutional_email','YES',field_text,0])
                    else:
                        variables.append([doc_id, 'not_institutional_email','NO','',0])
                else:
                    is_exist = "NO"
                    field_text = ''
            else:
                is_exist = "NO"
                field_text = ''

            field_count = 0
        else:
            continue
            field_count = 0
       # all_items = [doc_id,field,is_exist,field_text,field_count]
        #variables.append(','.join([str(i) for i in all_items]))
        variables.append((doc_id,field,is_exist,field_text,field_count))

    if len(dictt['references']) > 1:
        is_ref_heading = [dictt['references'].index(x) for x in dictt['references'] if x.lstrip('1234567890\s?\.?-? ٓ?~?\(?\)?\/?\s?').strip(':|.| |-|–| ٓ') in reference_headings]
        is_marg_heading = [dictt['references'].index(x) for x in dictt['references'] if x.lstrip('1234567890\s?\.?-? ٓ?~?\(?\)?\/?\s?').strip(':|.| |-|–| ٓ') in margin_headings]
        if is_marg_heading and not is_ref_heading:
            ref_headings_ind = is_marg_heading
            ref_headings = [x for x in dictt['references'] if x.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').strip(':|.| |-|–') in margin_headings]
        elif is_ref_heading:
            ref_headings_ind = is_ref_heading
            ref_headings = [x for x in dictt['references'] if x.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').strip(':|.| |-|–') in reference_headings]
        else:
            ref_headings_ind = is_ref_heading
            
        if len(ref_headings_ind) > 0:
            variables.append([doc_id,'reference_heading',"YES",ref_headings[0],0])
            if len(ref_headings_ind) > 1:
                if (ref_headings_ind[-2] +1) != ref_headings_ind[-1]:
                    ind = ref_headings_ind[0]+1
                else:
                    ind = ref_headings_ind[-1]+1
            else:
                ind = ref_headings_ind[0]+1

            references = dictt['references'][ind:]
            references = [x for x in references if x.lstrip('1234567890\s?\.?-? ٓ?~?\(?\)?\/?\s?').strip(':|.| |-|–| ٓ') not in reference_headings and x.lstrip('1234567890\s?\.?-? ٓ?~?\(?\)?\/?\s?').strip(':|.| |-|–| ٓ') not in margin_headings]
            ref_margin_joined = [x for x in ref_headings if x.lstrip('1234567890\s?\.?-? ٓ?~?\(?\)?\/?\s?').strip(':|.| |-|–| ٓ') in ref_margin]
            if len(ref_margin_joined) >0:
                variables.append([doc_id,'ref_margin_is_joined','YES',ref_margin_joined[0],0])
            else:
                nb_references = len(references)
#                 variables.append([doc_id,'nb_references',"YES",'',nb_references])
        else:
            variables.append([doc_id,'reference_heading',"NO",'',0])
            references = [x for x in dictt['references'] if len(x.split(' ')) > 2]

            nb_references = len(references)
#             variables.append([doc_id,'nb_references',"YES",'',nb_references])
       # print(references)
       
        ref_years = []
        fifteen_two_two = []
        two_thousand_fifteen = []
        before_2000 = []

        pat = re.compile('19\d{2}|20[0-2][0-9]')
        for x in references:
            xx = pat.findall(x)
            if len(xx) > 0:
                ref_years.append(int(xx[0]))
                if int(xx[0]) >= 2015:
                    fifteen_two_two.append(int(xx[0]))
                elif int(xx[0]) < 2015 and int(xx[0]) >= 2000:
                    two_thousand_fifteen.append(int(xx[0]))
                else:
                    before_2000.append(int(xx[0]))

        if len(ref_years) > 0:
            #range_years = max(ref_years)-min(ref_years)
            years_ranges = '2022-2015: {}, 2014-2000: {}, before_2000: {}'.format(len(fifteen_two_two),
                                                                                 len( two_thousand_fifteen),
                                                                                  len(before_2000))
            variables.append([doc_id,'ref_years',"YES",years_ranges,0])
        else:
            variables.append([doc_id,'ref_years',"NO",'',0])


        ref_links = []
        pat = re.compile('https?://[^\s]+|www.[^\s]+')
        for x in references:
            xx = pat.findall(x)
            if len(xx) > 0:
                ref_links.append(xx)
        if len(ref_links) > 0:
            variables.append([doc_id,'ref_links',"YES",'',len(ref_links)])
        else:
            variables.append([doc_id,'ref_links',"NO",'',0])
        
        ref_not_in_text = []
        ref_with_error = {}
#         if check_apa:
        not_ref = []
        for x in references:
            try:
                not_in_text,error,type_ = apa_style(x,all_text,cities,countries)
            except:
                print(x)
                not_ref.append(x)
                continue
            if not_in_text:
                ref_not_in_text.append(x)
            if error:
                ref_with_error[x] = type_
        if ref_not_in_text:
            variables.append([doc_id,'ref_not_in_text',"YES",'<br /><br />'.join(ref_not_in_text),len(ref_not_in_text)])
        if ref_with_error:
            json_ref_with_error = json.dumps(ref_with_error,ensure_ascii = False,indent='<br /><br />')
            variables.append([doc_id,'ref_with_error',"YES",json_ref_with_error,len(ref_with_error)])
#         elif check_ama:
            
#         elif check_mla:
        
        non_ara_refs = []
        for ref in references:
            try:
               if detect(re.sub('\d*|\(|\)|،|,|\?|-',"",ref)) != 'ar' and detect(re.sub('\d*|\(|\)|،|,|\?|-',"",ref)) != 'fa':
                    non_ara_refs.append(ref)
            except Exception as e:
               print(e)
               pass
        #non_ara_refs = [x for x in references if detect(re.sub('\d*|\(|\)|،|,',"",x)) != 'ar' if detect(re.sub('\d*|\(|\)|،|,',"",x)) != 'fa']
        
        references = [x for x in references if x not in not_ref]
        nb_non_ara_refs = len(non_ara_refs)
        if nb_non_ara_refs > 0:
            variables.append([doc_id,'nb_non_ara_refs',"YES",'',nb_non_ara_refs])
        else:
            variables.append([doc_id,'nb_non_ara_refs',"NO",'',0])
            
    insert_fields(variables)
#     dist_path = file_name.split(".docx")[0]+'.xlsx'
#     all_results_df.to_excel(dist_path,index=False)
