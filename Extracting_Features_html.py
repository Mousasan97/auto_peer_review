import codecs
from bs4 import BeautifulSoup
import pickle
import cssutils
import unicodedata
import pandas as pd
import numpy as np
import re
from collections import OrderedDict
import langid

class ExtractFeatures():
    
    def __init__(self,path_to_html,path_to_ara_authors,path_to_eng_authors,
                                    path_to_ara_aff,path_to_eng_aff):
        
        f = codecs.open(path_to_html, 'r')
        self.soup = BeautifulSoup(f.read(),'lxml')
        self.path_to_html = path_to_html
                 
        with open(path_to_ara_authors, "rb") as f:
            self.ara_authors = pickle.load(f)   
        
        with open(path_to_eng_authors, "rb") as f:
            self.eng_authors = pickle.load(f)
            
        with open(path_to_ara_aff, "rb") as f:
            self.ara_aff = pickle.load(f)
        
        with open(path_to_eng_aff, "rb") as f:
            self.eng_aff = pickle.load(f)
                        
    def get_paragraphs_and_tables(self):
        
        all_paragraphs = self.soup.findAll('p')
        all_paragraphs = [x for x in all_paragraphs if x.get_text().strip() != '']
        
        tables = self.soup.findAll('table')
        
        tables_par = []
        for table in tables:
            for par in table.findAll('p'):
                tables_par.append(par)
        tables_par = [x for x in tables_par if x.get_text().strip() != '']        
        
        for i in range(len(tables_par)):
            try:
                if all_paragraphs.index(tables_par[i]) >10:
                    all_paragraphs.remove(tables_par[i])
            except:
                pass
        tables_par = [x for x in tables_par if x not in all_paragraphs]
#         print(len(tables_par))
        
#         print('There are {} paragraphs in docx'.format(len(all_paragraphs)))
#         print('There are {} tables in docx'.format(len(tables_par)))
        
        return all_paragraphs,tables_par
   
    def remove_unicode(self,text):
        return unicodedata.category(text) == 'Co'
    
    def extract_features_from_par(self,all_paragraphs):
        
        text_and_features = OrderedDict()

        zone = 0
        for par in all_paragraphs:
            is_footnote = False

            partial_text = []
            is_next_zone = False
            partial_text_dict ={}
            par_style_dict = par.attrs

            all_fonts_dict ={}
            for f in par.findAll('font'):
                all_fonts_dict.update(f.attrs)

            in_zone_order = 0
            got_text = []
            for s in par:
                if is_next_zone:
#                     print('seperating zones')
                    
                    all_pt = "".join(partial_text)
#                     print(all_pt)
                    text_and_features[all_pt] = partial_text_dict
                    zone+=1
                    
                    partial_text = []
                    is_next_zone = False
                    partial_text_dict ={}
                    in_zone_order = 0
                    
                try:
                    try:
                        if s['class'][0] == 'sdendnotesym':
                            is_footnote = True
                            in_zone_order +=1
                            continue
                    except:
                        pass
                    try:
                        text = s.get_text()
                        if text.strip() == '':
                            in_zone_order +=1
                            continue
                    except:
                        in_zone_order +=1
                        continue
                        
                    if '\n\n' in text or re.compile(r' {4,}').findall(text):
                        
                        is_next_zone = True
                        
                    text = re.sub(r'\n'," ",text)
                    text = re.sub(r'\t'," ",text)
                    text = re.sub(r'\s{2,}',r" ",text)
                    text = unicodedata.normalize("NFKD",text)
                    text = "".join([char for char in text if not self.remove_unicode(char)])
                    partial_text.append(text)

                    partial_text_dict[text] = par_style_dict.copy()

                    for ff in s.findAll('span'):
                        partial_text_dict[text].update(ff.attrs)
                    if is_footnote:
                        partial_text_dict[text].update({'is_footnote':'1'})
                    if s.findAll('b'):
                        partial_text_dict[text].update({'is_bold':'1'})

                    partial_text_dict[text].update(all_fonts_dict.copy())
                    partial_text_dict[text].update({'zone':zone})

                    try:
                        if partial_text_dict[text]['lang'] == 'x-none':

                            if langid.classify(text)[0]!='en' and langid.classify(text)[0]!='ar' and langid.classify(text)[0]!= 'fa':
                                partial_text_dict[text]['lang'].update('fr')
                            elif langid.classify(text)[0] == 'fa':
                                partial_text_dict[text]['lang'].update('ar')
                            else:
                                partial_text_dict[text]['lang'].update(langid.classify(text)[0])      
                    except:

                        if langid.classify(text)[0] != 'en' and langid.classify(text)[0] != 'ar' and langid.classify(text)[0] != 'fa':
                            partial_text_dict[text]['lang'] = 'fr'
                        elif langid.classify(text)[0] == 'fa':
                            partial_text_dict[text]['lang'] = 'ar'
                        else:
                            partial_text_dict[text]['lang'] = langid.classify(text)[0]

                    partial_text_dict[text].update({'in_zone_order':in_zone_order})

                    in_zone_order +=1

                except Exception as e:
                    print(e)
#                     print('no text')
                    continue

            all_text = "".join(partial_text)
            all_text = re.sub(r'\n'," ",all_text)
            all_text = re.sub(r'\t'," ",all_text)
            all_text = re.sub(r'\s{2,}',r" ",all_text)
            all_text = unicodedata.normalize("NFKD", all_text)

            text_and_features[all_text] = partial_text_dict
            zone+=1
    
        df = pd.DataFrame.from_dict(text_and_features).T.stack().to_frame()
        df = pd.DataFrame(df[0].values.tolist(), index=df.index)
        df = df.sort_values(['zone','in_zone_order'])
        
        if 'class' in df.columns:
            df = df.drop('class',axis=1)
        if 'size' in df.columns:
            df = df.drop('size',axis=1)   
        if 'color' in df.columns:
            df = df.drop('color',axis=1) 
                
        return df
    
    def prep_face_feature(self,df):

        df['face'].fillna(df['face'].value_counts().index[0],inplace = True)
        df['face'] = df['face'].apply(lambda x: x.split(',')[0].strip())
        return df

    def prep_style_feature(self,df):
        
        if len(df[df['style'].isnull()])>0:
            for i in df[df['style'].isnull()].index:
                ii = df.index.get_loc(i)
                try:
                    if df.iloc[ii]['zone'] == df.iloc[ii-1]['zone']:
                        new_style = df.iloc[ii-1]['style']
                        df.iloc[ii,df.columns.get_loc('style')]= new_style
                    elif df.iloc[ii]['zone'] == df.iloc[ii+1]['zone']:
                        new_style = df.iloc[ii+1]['style']
                        df.iloc[ii,df.columns.get_loc('style')]= new_style
                except Exception as e:
                    try:
                        if df.iloc[ii]['zone'] == df.iloc[ii+1]['zone']:
                            new_style = df.iloc[ii+1]['style']
                            df.iloc[ii,df.columns.get_loc('style')]= new_style
                    except:
                        df.iloc[ii,df.columns.get_loc('style')] = df['style'].value_counts().index[0]
                if pd.isna(df.iloc[ii]['style']):
                    df.iloc[ii,df.columns.get_loc('style')] = df['style'].value_counts().index[0]
            
        for i in df[df['style'].str.contains(r'margin|background|decoration|indent|normal',regex=True)].index:
            ii = df.index.get_loc(i)
            try:
                if df.iloc[ii]['zone'] == df.iloc[ii-1]['zone']:
                    new_style = df.iloc[ii-1]['style']
                    df.iloc[ii,df.columns.get_loc('style')]= new_style
                elif df.iloc[ii]['zone'] == df.iloc[ii+1]['zone']:
                    new_style = df.iloc[ii+1]['style']
                    df.iloc[ii,df.columns.get_loc('style')]= new_style
            except Exception as e:
                try:
                    if df.iloc[ii]['zone'] == df.iloc[ii+1]['zone']:
                        new_style = df.iloc[ii+1]['style']
                        df.iloc[ii,df.columns.get_loc('style')]= new_style
                except:
                    df.iloc[ii,df.columns.get_loc('style')] = df['style'].value_counts().index[0]
            iii = 0        
            while re.compile('margin|background|decoration|indent|normal').findall(df.iloc[ii]['style']):
            
                df.iloc[ii,df.columns.get_loc('style')] = df['style'].value_counts().index[iii]
                iii+=1
       
        df['style'] = df['style'].apply(lambda x: re.compile('\d+').findall(x)[0])
       
        return df

    def prep_lang_feature(self,df):
        df['lang'] = df['lang'].apply(lambda x: x.split('-')[0])
        df['lang'] = np.where(df['lang'] =='fr', 'en', df['lang'])
        return df

    def prep_bold_feature(self,df):
        try:
            df['is_bold'].fillna(0,inplace = True)
        except:
            pass
        return df

    def prep_footnote_feature(self,df):
        try:
            df['is_footnote'].fillna(0,inplace = True)
        except:
            pass
        return df

    def prep_direction_feature(self,df):
        if 'dir' not in df.columns:
            df['dir'] = np.nan
        for i in df[df['dir'].isnull()].index:
            ii = df.index.get_loc(i)
            if df.iloc[ii]['perc_ara_words'] >= 50:
                 df.iloc[ii,df.columns.get_loc('dir')] = 'rtl'
            else:
                df.iloc[ii,df.columns.get_loc('dir')] = 'ltr'     
        return df

    def prep_align_feature(self,df):
        for i in df[df['align'].isnull()].index:
            ii = df.index.get_loc(i)
            if df.iloc[ii]['perc_ara_words'] >= 50:
                 df.iloc[ii,df.columns.get_loc('align')] = 'right'
            else:
                df.iloc[ii,df.columns.get_loc('align')] = 'left'
        return df

    def get_word_counts(self,df):

        word_counts = []
        for a in df.index:
            word_counts.append(len(a[1].split()))
        df['word_count'] = word_counts
        return df

    def word_counts_features(self,df):

        df['zone_word_count'] = df.groupby('zone')['word_count'].transform(lambda x:x.sum())
        df['word_perc'] = df.groupby('zone')['word_count'].apply(lambda x: x / float(x.sum()))
        df['perc_lang'] = df.groupby(['zone','lang'])['word_perc'].transform(lambda x: sum(x))
        df['perc_ara_words'] = np.where(df['lang'] =='ar', df['perc_lang'], np.nan)
        df['perc_eng_words'] = np.where(df['lang'] !='ar', df['perc_lang'], np.nan)
        dict_ara_mode = df.groupby("zone")['perc_ara_words'].apply(lambda x:x.mode()).to_dict()
        dict_ara_mode = {k[0]:v for k,v in dict_ara_mode.items()}
        df['perc_ara_words'].fillna(df.zone.map(dict_ara_mode),inplace = True)
        df['perc_ara_words'].fillna(0,inplace = True)
        dict_eng_mode = df.groupby("zone")['perc_eng_words'].apply(lambda x:x.mode()).to_dict()
        dict_eng_mode = {k[0]:v for k,v in dict_eng_mode.items()}
        df['perc_eng_words'].fillna(df.zone.map(dict_eng_mode),inplace = True)
        df['perc_eng_words'].fillna(0,inplace = True)
        df['perc_bold_words'] = df[df['is_bold'] == '1'].groupby('zone')['word_perc'].transform(lambda x: x.sum())
        dict_count_bold = df.groupby("zone")['perc_bold_words'].apply(lambda x:x.mode()).to_dict()
        dict_count_bold = {k[0]:v for k,v in dict_count_bold.items()}
        df['perc_bold_words'].fillna(df.zone.map(dict_count_bold),inplace = True)
        df['perc_bold_words'].fillna(0,inplace = True)

        return df

    def contains_names(self,df):

        contains_ara_authors = []
        for a in df['level_0'].to_list():
            names_found = [x for x in self.ara_authors if(x in a.title().split())]
            if names_found:
                if len(names_found)/len(a.split()) > 0.2:
#                     print(names_found)
#                     print(len(names_found)/len(a[1].split()))
#                     print(a[1])
                    contains_ara_authors.append(1)
                else:
                    contains_ara_authors.append(0)
            else:
                    contains_ara_authors.append(0)

        df['contains_ara_authors'] = contains_ara_authors

        contains_eng_authors = []
        for a in df['level_0'].to_list():
            names_found = [x for x in self.eng_authors if(x in a.title().split())]
            if names_found:
                if len(names_found)/len(a.split()) > 0.2:
#                     print(names_found)
#                     print(len(names_found)/len(a[1].split()))
#                     print(a[1])
                    contains_eng_authors.append(1)
                else:
                    contains_eng_authors.append(0)
            else:
                    contains_eng_authors.append(0)

        df['contains_eng_authors'] = contains_eng_authors

        return df

    def contains_emails(self,df):

#         contains_email = []
#         for a in df.index:
#             match = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', a[1])
#             if match:
#                 contains_email.append(1)
#             else:
#                 contains_email.append(0)

#         df['contains_email'] = contains_email
      
        df['contains_email'] = np.where(df['level_0'].str.contains('[\w.+-]+@[\w-]+\.[\w.-]+'), 1, 0)
        return df

    def contains_aff(self,df):

        contains_ara_aff = []
        for a in df['level_0'].to_list():
            aff = [x for x in self.ara_aff if(x in a.title().strip())]
            if aff:
                contains_ara_aff.append(1)
            else:
                contains_ara_aff.append(0)

        df['contains_ara_aff'] = contains_ara_aff

        contains_eng_aff = []
        for a in df['level_0'].to_list():
            aff = [x for x in self.eng_aff if(x in a.title().strip())]
            if aff:
                contains_eng_aff.append(1)
            else:
                contains_eng_aff.append(0)

        df['contains_eng_aff'] = contains_eng_aff

        return df

    def drop_unwanted_features(self,df):

        df.reset_index(inplace=True)
        df = df.drop(['word_perc','perc_lang','in_zone_order',
                     'lang','is_bold','level_1'],axis=1)
        
        try: #sometimes there is "id" in the style attrs of the html
            df =df.drop(['id'],axis=1)
        except:
            pass
            
        return df

    def single_word_feature(self,df):
        df['is_single_word'] = np.where(df['level_0'].str.split( ).str.len() == 1, 1,0)
        return df

    def elongated_words_feature(self,df):
        df['elongated_words'] = df['level_0'].apply(lambda x: len(re.compile('[\w]*ـ').findall(x)))
        return df
    
    def add_zone_lang(self,df):
        
        df['is_ara'] = np.where(df['perc_ara_words'] >= 0.4, 1, 0)
        
        df = df.drop(['perc_ara_words','perc_eng_words'],axis=1)
       
        return df
    
    def add_is_max_font(self,df):
        
        max_font = df['style'].astype(int).max()
        df['is_max_font'] = np.where(df['style'] == str(max_font), 1, 0)
        
        return df
    
    def contains_other_keyword(self,df):
        
        df['contains_other_keywords'] = np.where(df['level_0'].str.contains(r'[Kk]ey\s?[Ww]ords?\s*:?'), 1, 0)
        return df
    
    def remove_elongation(self,df):
        df['level_0'] = df['level_0'].apply(lambda x: re.sub(r'ـ','',x))
        df['level_0'] = df['level_0'].apply(lambda x: re.sub(r'\(?\*+\)?',' ',x))
        return df
    
    def contains_main_keyword(self,df):
        df['contains_main_keywords'] = np.where(df['level_0'].str.contains(r'(كلمات|الكلمات)\s*(المفتاحيّة|المفتاحيّه|الداله|الاصطلاحية|المفتاحيه|مفتاحية|مفتاحيّة|المفتاح|الافتتاحية|مفتاحيه|المفتاحية|الدالة)'),1,0)
        return df
    
    def contains_date_format(self,df):
        
        contains_date_format = []
        for a in df['level_0'].to_list():
            x = re.search("([1-9]|0[1-9]|1[0-9]|2[0-9]|3[0-1])\s?(\.|-|/)\s?([1-9]|0[1-9]|1[0-2])\s?(\.|-|/)\s?([0-9][0-9]?|19[0-9][0-9]|20[0-9][0-9])$|([0-9][0-9]?|19[0-9][0-9]|20[0-9][0-9])\s?(\.|-|/)\s?([1-9]|0[1-9]|1[0-2])\s?(\.|-|/)\s?([1-9]|0[1-9]|1[0-9]|2[0-9]|3[0-1])|(\d{1,2})\s?,?\s?(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s?,?\s+(\d{4})|(\d{1,2})\s?,?\s?(يناير|فبراير|مارس|ابريل|مايو|يونيو|يوليو|أغسطس|سبتمبر|أكتوبر|نوفمبر|ديسمبر)\s?,?\s+(\d{4})|(\d{1,2})\s?,?\s?(كانون الثاني|شباط|آذار|نيسان|أيار|حزيران|تموز|آب|أيلول|تشرين الأول|تشرين الثاني|كانون الأول)\s?,?\s+(\d{4})",a)
            try:
                x.group()
                contains_date_format.append(1)
            except:
                contains_date_format.append(0)
        df['contains_date_format'] = contains_date_format
        return df
        
    def contains_abstract_keywords(self,df):
        
        df['contains_main_abstract'] = np.where(df['level_0'].str.contains(r'تلخيص|هدف البحث|مُلخَّص|ملخّص|المُلخَّص|يتناول البحث|هدفت الدراسة إلى|خلاصة البحث|مستخلص|ملخص|الملخص|المستخلص|الخلاصة'),1,0)
        
        df['contains_other_abstract'] = np.where(df['level_0'].str.contains(r'[Aa][Bb][Ss][Tt][Rr][Aa][Cc][Tt]|[Ss][Uu][Mm][Mm][Aa][Rr][Yy]|English [Ss]ummary'),1,0)
  
        return df

    def contains_abs_is_single(self,df):
        df['contains_abs_is_single'] = np.where(((df['contains_main_abstract'] == 1)|(df['contains_other_abstract'] == 1)) & df['is_single_word'] == 1,1,0)
        
        return df
    
    def contains_date_keywords(self,df):
    
        df['contains_date_keywords'] = np.where(df['level_0'].str.contains(r'قبول البحث|قبول النشر|تأريخ القبول|تم قبول البحث بتاريخ|تسليم البحث|استلم البحث في|Published:|Accepted:|Received:|Accepted in|Received in|قبل البحث في|تاريخ المراجعة|تاريخ قبول|اريخ وصول البحث|تاريخ قبول البحث|تاريخ تقديم البحث|تاريخ الاستلام|تاريخ الإرسال|تاريخ النشر|تاريخ القبول'),1,0)
        
        return df
    
    def contains_issn_doi(self,df):
        
        df['contains_issn_doi'] = np.where(df['level_0'].str.contains(r'[Ii][Ss][Ss][Nn]|[Dd][Oo][Ii]'), 1, 0)
        return df
    
    def is_first_page(self,df):
    
        df['Zone'] = list(df.index)
        df['is_first_page'] = np.where(df['Zone']<15, 1, 0)
        df['first_itmes'] = np.where(df['Zone']< 5, 1, 0)
        df['first_zone'] = np.where(df['Zone'] == 0, 1, 0)
        df['second_zone'] = np.where(df['Zone'] == 1, 1, 0)
        
        return df
    
    def contains_reference_keywords(self,df):
    
        df['contains_reference_keywords'] = np.where(df['level_0'].str.contains(r'الإحالات|الهوامش|المراجع|المصادر'), 1, 0)

        return df
    
    def starts_with_nb(self,df):
    
        df['starts_with_nb'] = np.where(df['level_0'].str.contains(r'\A\s?\d'), 1, 0)
        
        return df
    
    def contains_url(self,df):
    
        df['contains_url'] = np.where(df['level_0'].str.contains(r'(https?://[^\s]+)'), 1, 0)
        
        return df
    
    def count_aff_keywords(self,df):
        
        df['count_aff_keywords'] = df['level_0'].str.count(r'معهد|[Un][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy]|جامعة|الجامعة|قسم|كلية|[Dd][Ee][Pp][Aa]?[Rr]?[Tt]?[Mm]?[Ee]?[Nn]?[Tt]?|College|الجامعي')
        
        return df
    
    def contains_ara_aff_keywords(self,df):
    
        df['contains_ara_aff_keywords'] = np.where(df['level_0'].str.contains(r'جامعة|الجامعة|قسم|معهد|كلية|كليه|الجامعي'),1,0)

        return df
    
    def contains_eng_aff_keywords(self,df):
    
        df['contains_eng_aff_keywords'] = np.where(df['level_0'].str.contains(r'[Un][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy]|College|[Dd][Ee][Pp][Aa]?[Rr]?[Tt]?[Mm]?[Ee]?[Nn]?[Tt]?'),1,0)

        return df
    
    def contains_eng_aff_tags(self,df):
        
        df['contains_eng_aff_tags'] = np.where(df['level_0'].str.contains(r'D[Rr]\s?\.|[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Aa][Ss][Ss][Ii][Ss][Tt]?[Aa]?[Nn]?[Tt]?\.?\s[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Pp]\.?[Hh]\.?[Dd]\.?'),1,0)

        return df
    
    def contains_ara_aff_tags(self,df):
        
        df['contains_ara_aff_tags'] = np.where(df['level_0'].str.contains(r'(?<!\w)أ\s?[/\.]|(?<!\w)د\s?[/\.]|أستاذ|أستاذ مساعد|طالب ماجستير|طالب دكتوراة'),1,0)

        return df
    
    def count_aff_tags(self,df):
        
        df['count_aff_tags'] = df['level_0'].str.count(r'(?<!\w)أ\s?[/\.]|(?<!\w)د\s?[/\.]|Dr\s?\.|[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Aa][Ss][Ss][Ii][Ss][Tt]?[Aa]?[Nn]?[Tt]?\.?\s[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Pp][Hh][Dd]\.?|أستاذ مساعد')
        
        return df
    
    def tables_figures_keywords(self,df):
    
        df['tables_figures_keywords'] = np.where(df['level_0'].str.contains(r'ا?ل?شكل\s?\(?\d+\)?|ا?ل?جدول\s?\(?\d+\)?|ا?ل?شكل ا?ل?رقم|ا?ل?جدول ا?ل?رقم'),1,0)

        return df
    
    def count_punctuation(self,df):
        
        df['count_punctuation'] = df['level_0'].str.count(r'\.|,|،|:|-')

        return df
    
    def relative_count_punctuation(self,df):
    
        df['relative_count_punctuation'] = df['level_0'].str.count(r'\.|,|،|:|-')/df['zone_word_count']
        df['relative_count_punctuation'].fillna(0,inplace = True)
        
        return df
    
    def reference_keyword(self,df):
        
        df['reference_keyword'] = np.where(df['level_0'].str.contains(r'المصادر|المراجع|المصدر|الإحالات|الهوامش'),1,0)
        
        return df
    
    def par_year(self,df):

        df['par_year'] = np.where(df['level_0'].str.contains(r'\(\s?\d{4}\s?\)'), 1, 0)
        
        return df
    
    def contains_mag_keywords(self,df):
        df['contains_mag_keywords'] = np.where(df['level_0'].str.contains('مجلة'),1,0)
        
        return df
    
    def get_all_features(self):

        pars,tables = self.get_paragraphs_and_tables()
 
        df = self.extract_features_from_par(pars)
        
        df = self.prep_face_feature(df)
        
        df = self.prep_style_feature(df)
        
        df = self.prep_lang_feature(df)
        
        df = self.prep_bold_feature(df)
        
        df = self.prep_footnote_feature(df)

        df = self.get_word_counts(df)
        
        df = self.word_counts_features(df)
        
        df = self.prep_direction_feature(df)
        
        df = self.prep_align_feature(df)
        
        df = self.drop_unwanted_features(df)
        
        df = df.groupby(['zone']).agg(lambda x:x.value_counts().index[0])
        
        df = self.single_word_feature(df)
        
#         df = self.elongated_words_feature(df)
        
        df = self.add_zone_lang(df)
        
        df = self.remove_elongation(df)
        
        df = self.contains_names(df)
        
        df = self.contains_aff(df)
        
        df = self.contains_emails(df)
        
        df = self.add_is_max_font(df)
        
        df = self.contains_other_keyword(df)
        
        df = self.contains_main_keyword(df)
        
        df = self.contains_date_format(df)
        
        df = self.contains_abstract_keywords(df)
        
        df = self.contains_date_keywords(df)
        
        df = self.contains_issn_doi(df)
        
        df = self.is_first_page(df)
        
        df = self.contains_reference_keywords(df)
        
        df = self.starts_with_nb(df)
        
        df = self.contains_url(df)
        
        df = self.contains_ara_aff_keywords(df)
        
        df = self.contains_eng_aff_keywords(df)
        
        df = self.count_aff_keywords(df)
        
        df = self.contains_eng_aff_tags(df)
        
        df = self.contains_ara_aff_tags(df)
        
        df = self.count_aff_tags(df)
        
        df = self.tables_figures_keywords(df)
        
#         df = self.count_punctuation(df)
        
        df = self.relative_count_punctuation(df)
        
        df = self.contains_abs_is_single(df)
        
        df = self.reference_keyword(df)
        
        df = self.par_year(df)
        
        df = self.contains_mag_keywords(df)
        
        df['sub_label'] = np.nan
        df['labels'] = np.nan

#         df['path_to_txt'] = 'docx_to_txt/'+self.path_to_html.split('/')[1].split('.')[0]+'.txt'

        return df
            
    
            