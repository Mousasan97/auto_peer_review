import codecs
from bs4 import BeautifulSoup
import pickle
import cssutils
import logging
cssutils.log.setLevel(logging.CRITICAL)

import unicodedata
import pandas as pd
import numpy as np
import re
from collections import OrderedDict
import langid
from langdetect import detect
import pyarabic.araby as araby

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

        all_text = self.soup.findAll(re.compile('^p|^h[1-6]$|^div'))
        all_text = [x for x in all_text if x.get_text().strip() != '']

        tables = self.soup.findAll('table')

        tables_par = []
        for table in tables:
            for par in table.findAll('p'):
                tables_par.append(par)
        tables_par = [x for x in tables_par if x.get_text().strip() != '']

        for i in range(len(tables_par)):
            try:
                if all_text.index(tables_par[i]) >12 and len(tables_par[i].get_text().strip().split( )) < 3:
                    all_text.remove(tables_par[i])
            except:
                pass
        tables_par = [x for x in tables_par if x not in all_text]

        return all_text,tables_par

    def remove_unicode(self,text):
        return unicodedata.category(text) == 'Co'

    def get_selectors_dict(self):

        selectors = {}
        for styles in self.soup.select('style'):
            css = cssutils.parseString(styles.encode_contents())
            for rule in css:
                if rule.type == rule.STYLE_RULE:
                    style = rule.selectorText
                    selectors[style] = {}
                    for item in rule.style:
                        propertyname = item.name
                        value = item.value
                        selectors[style][propertyname] = value
        return selectors

    def extract_features_from_par(self,all_paragraphs,selectors):

        text_and_features = OrderedDict()
        zone =0
        for par in all_paragraphs:

            update_style = False
            is_footnote = False
            is_next_zone = False
            zone_changed = False
            split_text = False

            partial_text = []
            partial_text_dict ={}

            try:
                par_style_dict = selectors["."+par['class'][0]]
            except:
#                 print('no style class')
#                 print(zone)
                zone+=1
                continue

            if par.find('a',href = re.compile('mailto:')):
                print('splitting emails')
                emails = [x.extract() for x in par.findAll('a',href = re.compile('mailto:'))]
                print('emails: {}'.format(emails))
                for email in emails:
                    try:
                        text = email['href'].split(':')[-1]
                        text = text.strip('%20')
                    except:
                        try:
                            text = email.get_text()
                        except:
                            text = str(email)
                    text = re.sub(r'\n'," ",text)
                    text = re.sub(r'\t'," ",text)
                    text = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',text)
                    text = re.sub(r'\s{2,}',r" ",text)
                    text = re.sub('أ','أ',text)
                    text = re.sub('ئ','ئ',text)
                    text = re.sub('\xa0',' ',text)
                    text = "".join([char for char in text if not self.remove_unicode(char)])
                    if text.strip():
                        try:
                            style_dict = selectors["."+email['class'][0]]
                            try:
                                if "%" in selectors["."+email['class'][0]]['font-size']:
                                    del selectors["."+email['class'][0]]['font-size']
                                    style_dict = selectors["."+email['class'][0]]
                            except:
                                pass
                            partial_text_dict[text].update(style_dict.copy())
                        except:
                            pass
                        if text not in text_and_features:
                            partial_text_dict[text] = par_style_dict.copy()
                            partial_text_dict[text].update({'zone':zone})
                            partial_text_dict[text].update({'in_zone_order':0})
                            partial_text_dict[text]['lang'] = 'en'
                            text_and_features[text] = partial_text_dict
                            zone+=1
                        partial_text_dict ={}

            if par.name == 'h1':
                text = par.get_text().strip()
                text = re.sub(r'\n'," ",text)
                text = re.sub(r'\t'," ",text)
                text = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',text)
                text = re.sub(r'\s{2,}',r" ",text)
                text = re.sub('أ','أ',text)
                text = re.sub('ئ','ئ',text)
                text = re.sub('\xa0',' ',text)
                text = "".join([char for char in text if not self.remove_unicode(char)])
                partial_text_dict[text] = par_style_dict.copy()
                partial_text_dict[text].update({'zone':zone})
                partial_text_dict[text].update({'in_zone_order':0})
                if langid.classify(text)[0] != 'en' and langid.classify(text)[0] != 'ar' and langid.classify(text)[0] != 'fa':
                    partial_text_dict[text]['lang'] = 'fr'
                elif langid.classify(text)[0] == 'fa':
                    partial_text_dict[text]['lang'] = 'ar'
                else:
                    partial_text_dict[text]['lang'] = langid.classify(text)[0]

            spans = par.findAll('span')
            in_zone_order = 0
            for s in spans:
                if is_next_zone and s.get_text().strip()!= '':

                    all_pt = " ".join(partial_text)
                    all_pt = re.sub('\xa0',' ',all_pt)
                    all_pt = re.sub(r'\s{2,}',r" ",all_pt)
                    all_pt = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)','',all_pt)

                    text_and_features[all_pt] = partial_text_dict
                    zone+=1

                    partial_text = []
                    is_next_zone = False
                    zone_changed = True
                    partial_text_dict ={}
                    in_zone_order = 0
                try:
                    if s['class'][0] == 'footnodeNumber':
#                         print('is footnote')
                        is_footnote = True
                        in_zone_order +=1
                        continue
                except:
                    pass

                if s.find('a',id = re.compile('body_ftn')):
#                     print(s)
                    in_zone_order +=1
                    continue

                try:
                    text = s.get_text().strip()
                    text = unicodedata.normalize("NFKD",text)
                    if text.strip() == '':
                        if re.compile(r' {3,}').findall(text):
                            is_next_zone = True
                        in_zone_order +=1
                        continue

                except:
                    in_zone_order +=1
                    continue

                text = unicodedata.normalize("NFKD",text)
                text = re.sub('\xa0',' ',text.strip())
                
                if re.compile('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+').findall(text):
                    new_zones = re.compile('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+').findall(text)
                    zone_changed = True
                    split_text = True
                    for nz in new_zones:
                        text = re.sub(nz,'',text)
                
                if ('\n\n' in text or re.compile(r' {4,}').findall(text)) and len(re.compile('https?://[^\s]+|www.[^\s]+').findall(text))<1:
                    try:
                        if re.split(' {4,}',text)[1].strip() != '':
                            new_zones = re.split(' {4,}',text)[1:]
                            zone_changed = True
                            split_text = True
                    except:
                        pass
                    text = re.split(' {4,}',text)[0]
                    is_next_zone = True
                    
#                 if len(re.compile('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+').findall(text)) >0:
#                     print('splitting emails')
#                     emails = re.compile('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+').findall(text)
#                     text1 = ' '.join(emails)
#                     zone_changed = True
#                     split_text = True
                    
#                     text = re.split('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+',text)[0]
#                     is_next_zone = True 
                    
                text = re.sub(r'\n'," ",text)
                text = re.sub(r'\t'," ",text)
                text = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',text)
                text = re.sub(r'\s{2,}',r" ",text)
                text = re.sub('أ','أ',text)
                text = re.sub('ئ','ئ',text)
                text = re.sub('\xa0',' ',text)
                text = "".join([char for char in text if not self.remove_unicode(char)])

                partial_text.append(unicodedata.normalize("NFKD", text))

                try:
                    style_dict = selectors["."+s['class'][0]]
                    try:
                        if "%" in selectors["."+s['class'][0]]['font-size']:
                            del selectors["."+s['class'][0]]['font-size']
                        style_dict = selectors["."+s['class'][0]]
                    except:
                        pass
                except:
                    update_style = True
                    span_to_update = in_zone_order

                    partial_text_dict[text] = par_style_dict.copy()
                    partial_text_dict[text].update({'zone':int(zone)})
                    partial_text_dict[text].update({'in_zone_order':int(in_zone_order)})
                    in_zone_order +=1
                    continue

                partial_text_dict[text] = par_style_dict.copy()
                partial_text_dict[text].update(style_dict.copy())
                if is_footnote:
                    partial_text_dict[text].update({'is_footnote':'1'})
                if update_style:
                    try:
                        new_style = selectors["."+spans[span_to_update+1]['class'][0]]
                        tt_update = spans[span_to_update].get_text()
                        tt_update = re.sub(r'\n'," ",tt_update)
                        tt_update = re.sub(r'\t'," ",tt_update)
                        tt_update = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',tt_update)
                        tt_update = re.sub(r'\s{2,}',r" ",tt_update)
                        tt_update = re.sub('أ','أ',tt_update)
                        tt_update = re.sub('ئ','ئ',tt_update)
                        tt_update = re.sub('\xa0',' ',tt_update)
                        tt_update = "".join([char for char in tt_update if not self.remove_unicode(char)])

                        partial_text_dict[unicodedata.normalize("NFKD", tt_update)].update(new_style.copy())
                    except:
                        pass

                partial_text_dict[text].update({'zone':zone})
                partial_text_dict[text].update({'in_zone_order':in_zone_order})

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
                in_zone_order +=1

            if zone_changed:
                all_text = " ".join(partial_text)
            else:
                all_text = par.get_text()
            all_text = re.sub(r'\n'," ",all_text)
            all_text = re.sub(r'\t'," ",all_text)
            all_text = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',all_text)
            all_text = re.sub(r'\s{2,}',r" ",all_text)
            all_text = re.sub('أ','أ',all_text)
            all_text = re.sub('ئ','ئ',all_text)
            all_text = re.sub('\xa0',' ',all_text)
            all_text = unicodedata.normalize("NFKD", all_text)
            if len(partial_text_dict) == 0:
                partial_text_dict[all_text] = par_style_dict.copy()
                partial_text_dict[all_text].update({'zone':int(zone)})
                partial_text_dict[all_text].update({'in_zone_order':int(in_zone_order)})

            if all_text in text_and_features:
                text_and_features[all_text+' '] = partial_text_dict
            else:
                text_and_features[all_text] = partial_text_dict

            if split_text:
                for nt in new_zones:
                    if nt.strip() != '':
                        text1 = nt.strip()
                        text1 = re.sub(r'\n'," ",text1)
                        text1 = re.sub(r'\t'," ",text1)
                        text1 = re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',text1)
                        text1 = re.sub(r'\s{2,}',r" ",text1)
                        text1 = re.sub('أ','أ',text1)
                        text1 = re.sub('ئ','ئ',text1)
                        text1 = re.sub('\xa0',' ',text1)
                        text1 = "".join([char for char in text1 if not self.remove_unicode(char)])
                        zone+=1
                        in_zone_order += 1
                        partial_text_dict = {}

                        partial_text_dict[text1] = par_style_dict.copy()
                        partial_text_dict[text1].update(style_dict.copy())

                        partial_text_dict[text1].update({'zone':zone})
                        partial_text_dict[text1].update({'in_zone_order':in_zone_order})

                        if langid.classify(text1)[0] != 'en' and langid.classify(text1)[0] != 'ar' and langid.classify(text1)[0] != 'fa':
                            partial_text_dict[text1]['lang'] = 'fr'
                        elif langid.classify(text1)[0] == 'fa':
                            partial_text_dict[text1]['lang'] = 'ar'
                        else:
                            partial_text_dict[text1]['lang'] = langid.classify(text1)[0]
                        in_zone_order += 1
                        text_and_features[text1] = partial_text_dict
            zone+=1

        df = pd.DataFrame.from_dict(text_and_features).T.stack().to_frame()
        df = pd.DataFrame(df[0].values.tolist(), index=df.index)
        df = df.sort_values(['zone','in_zone_order'])

        columns_to_del = ['class','size','color','margin-bottom','margin-top','display',
                  'writing-mode','margin-left','margin-right','letter-spacing','border-color',
                  'background-color','text-decoration','line-height','padding-bottom','border-width',
                 'border-bottom-style','padding-top','border-top-style','vertical-align','border-style',
                 'padding-left','padding-right','border-left-style','border-right-style',
                         'text-shadow','padding','border-bottom-width','border-bottom-color',
                         'border-top-width','border-top-color']

        del_cols = [x for x in columns_to_del if x in df.columns]

        df = df.drop(del_cols,axis=1)

        return df

    def prep_font_family(self,df):

        df['font-family'].fillna(df['font-family'].value_counts().index[0],inplace = True)
        df['font-family'] = df['font-family'].apply(lambda x: x.split(',')[0].strip())

        return df

    def prep_font_size(self,df):

        if len(df[df['font-size'].isnull()]) > 0:
            for i in df[df['font-size'].isnull()].index:
                ii = df.index.get_loc(i)
                try:
                    if df.iloc[ii]['zone'] == df.iloc[ii-1]['zone']:
                        new_style = df.iloc[ii-1]['font-size']
                        df.iloc[ii,df.columns.get_loc('font-size')]= new_style
                    elif df.iloc[ii]['zone'] == df.iloc[ii+1]['zone']:
                        new_style = df.iloc[ii+1]['font-size']
                        df.iloc[ii,df.columns.get_loc('font-size')]= new_style
                except Exception as e:
                    try:
                        if df.iloc[ii]['zone'] == df.iloc[ii+1]['zone']:
                            new_style = df.iloc[ii+1]['font-size']
                            df.iloc[ii,df.columns.get_loc('font-size')]= new_style
                    except:
                        df.iloc[ii,df.columns.get_loc('font-size')] = df['font-size'].value_counts().index[0]
                if pd.isna(df.iloc[ii]['font-size']):
                    df.iloc[ii,df.columns.get_loc('font-size')] = df['font-size'].value_counts().index[0]

        df['font-size'] = df['font-size'].apply(lambda x: re.compile('\d+').findall(x)[0])
        df['font-size'] = df['font-size'].astype(int)

        return df

    def prep_indetation(self,df):

        if 'text-indent' in df.columns:
            df['text-indent'].fillna(0,inplace = True)
        else:
            df['text-indent'] = 0

        return df

    def prep_lang_feature(self,df):
        if 'lang' not in df.columns:
            df['lang'] = np.nan
#         df['lang'] = df['lang'].apply(lambda x: x.split('-')[0])
        for i in df[df['lang'].isnull()].index:
            ii = df.index.get_loc(i)
            if langid.classify(df.iloc[ii].name[0])[0]=='ar' or langid.classify(df.iloc[ii].name[0])[0]=='fa':
                df.iloc[ii,df.columns.get_loc('lang')] = 'ar'
            else:
                df.iloc[ii,df.columns.get_loc('lang')] = 'en'

        return df

    def prep_bold_feature(self,df):
        if 'font-weight' in df.columns:
            df['is_bold'] = np.where(df['font-weight'] == 'bold' ,1,0)
            df = df.drop('font-weight',axis=1)
        else:
            df['is_bold'] = 0

        return df

    def prep_font_style(self,df):
        if 'font-style' in df.columns:
            df['is_italic'] = np.where(df['font-style'] == 'italic' ,1,0)
            df = df.drop('font-style',axis=1)
        else:
            df['is_italic'] = 0

        return df

    def prep_footnote_feature(self,df):
        try:
            df['is_footnote'].fillna(0,inplace = True)
        except:
            pass
        return df

    def prep_direction_feature(self,df):
        if 'direction' not in df.columns:
            df['direction'] = np.nan
#         for i in df[df['direction'].isnull()].index:
#             ii = df.index.get_loc(i)
#             if df.iloc[ii]['perc_ara_words'] >= 0.5:
#                  df.iloc[ii,df.columns.get_loc('direction')] = 'rtl'
#             else:
#                 df.iloc[ii,df.columns.get_loc('direction')] = 'ltr'
        for i in df.index:
            ii = df.index.get_loc(i)
            if df.iloc[ii]['perc_ara_words'] >= 0.5:
                 df.iloc[ii,df.columns.get_loc('direction')] = 'rtl'
            else:
                df.iloc[ii,df.columns.get_loc('direction')] = 'ltr'
        return df

    def prep_align_feature(self,df):
        for i in df[(df['text-align'] != 'justify')&(df['text-align'] != 'center')].index:
            ii = df.index.get_loc(i)
            if df.iloc[ii]['perc_ara_words'] >= 0.5:
                 df.iloc[ii,df.columns.get_loc('text-align')] = 'right'
            else:
                df.iloc[ii,df.columns.get_loc('text-align')] = 'left'

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
        
        df['perc_italic'] = np.where(df['is_italic'] ==1, df['word_perc'], np.nan)
        dict_italics_mode = df.groupby("zone")['perc_italic'].apply(lambda x:x.mode()).to_dict()
        dict_italics_mode = {k[0]:v for k,v in dict_italics_mode.items()}
        df['perc_italic'].fillna(df.zone.map(dict_italics_mode),inplace = True)
        df['perc_italic'].fillna(0,inplace = True)

        df['perc_bold'] = np.where(df['is_bold'] ==1, df['word_perc'], np.nan)
        dict_italics_mode = df.groupby("zone")['perc_bold'].apply(lambda x:x.mode()).to_dict()
        dict_italics_mode = {k[0]:v for k,v in dict_italics_mode.items()}
        df['perc_bold'].fillna(df.zone.map(dict_italics_mode),inplace = True)
        df['perc_bold'].fillna(0,inplace = True)
        
        df['is_italic'] = np.where(df['perc_italic']>0.5, 1,0)
        df['is_bold'] = np.where(df['perc_bold']>0.5, 1,0)

        return df

    def contains_names(self,df):

        contains_ara_authors = []
        for a in df['level_0'].to_list():
            a = re.sub('[\.\,\،\_\-\–\:]',' ',a)
            a = re.sub('\s{2,}',' ',a)
            a = re.sub('(?<=\w)\d',' ',a)
            names_found = [x for x in self.ara_authors if(x in a.title().split())]
            if names_found:
                if len(names_found)/len(a.split()) > (1/3):
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
            a = re.sub('[\.\,\،\_\-\–\:]',' ',a)
            a = re.sub('\s{2,}',' ',a)
            a = re.sub('(?<=\w)\d',' ',a)
            names_found = [x for x in self.eng_authors if(x.title() in a.title().split())]
            if names_found:
                if len(names_found)/len(a.split()) > (1/3):
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
        df['contains_email'] = np.where((df['level_0'].str.contains('[\w.+-]+\s?@\s?[\w-]+\.[\w.-]+')|(df['level_0'].str.contains('@gmail|@hotmail|@aol|@yahoo|@qq.com|@msn'))), 1, 0)
        return df

    def contains_aff(self,df):

        contains_ara_aff = []
        perc_ara_aff = []
        for a in df['level_0'].to_list():
            a = re.sub('[\-\.\,\،\_\-]',' ',a)
            a = re.sub('\s{2,}',' ',a)
            aff = [x for x in self.ara_aff if(x in a.title().strip())]
            if len(aff)>0:
                contains_ara_aff.append(1)
            else:
                contains_ara_aff.append(0)
            try:
                perc_ara_aff.append(sum([len(x.split( )) for x in aff])/len(a.split()))
            except:
                perc_ara_aff.append(0)

        df['contains_ara_aff'] = contains_ara_aff
        df['perc_ara_aff'] = perc_ara_aff

        contains_eng_aff = []
        perc_eng_aff = []
        for a in df['level_0'].to_list():
            a = re.sub('[\.\,\،\_\-]',' ',a)
            a = re.sub('\s{2,}',' ',a)
            aff = [x for x in self.eng_aff if(x.title() in a.title().strip())]
            if aff:
                contains_eng_aff.append(1)
            else:
                contains_eng_aff.append(0)
            try:
                perc_eng_aff.append(sum([len(x.split( )) for x in aff])/len(a.split()))
            except:
                perc_eng_aff.append(0)

        df['contains_eng_aff'] = contains_eng_aff
        df['perc_eng_aff'] = perc_eng_aff

        return df

    def drop_unwanted_features(self,df):

        df.reset_index(inplace=True)
        df = df.drop(['word_perc','perc_lang','in_zone_order',
                     'lang','level_1','word_count','perc_bold','perc_italic'],axis=1)

        try: #sometimes there is "id" in the style attrs of the html
            df =df.drop(['id'],axis=1)
        except:
            pass

        return df

    def single_word_feature(self,df):
        df['is_single_word'] = np.where((df['level_0'].str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|–').str.split( ).str.len() <3)&(df['is_first_page'] == 1), 1,0)
        return df

    def elongated_words_feature(self,df):
        df['elongated_words'] = df['level_0'].apply(lambda x: len(re.compile('[\w]*ـ').findall(x)))
        return df

    def add_zone_lang(self,df):

#         df['is_ara'] = np.where(df['perc_ara_words'] >= 0.4, 1, 0)
        is_ara = []
        for a in df['level_0'].to_list():
            try:
                if langid.classify(a)[0]=='ar' or langid.classify(a)[0]=='fa' or langid.classify(a)[0]=='ur' or detect(a) == 'ar':
                    is_ara.append(1)
                else:
                    is_ara.append(0)
            except:
                if langid.classify(a)[0]=='ar' or langid.classify(a)[0]=='fa' or langid.classify(a)[0]=='ur':
                    is_ara.append(1)
                else:
                    is_ara.append(0)
        df['is_ara'] = is_ara
        df = df.drop(['perc_ara_words','perc_eng_words'],axis=1)

        return df

    def add_is_max_font(self,df):

        max_font = df['font-size'].astype(int).max()
        df['is_max_font'] = np.where(df['font-size'] == max_font, 1, 0)

        return df

    def contains_other_keyword(self,df):

        df['contains_other_keywords']= np.where(df['level_0'].str.contains(r'[Kk]ey\s?[Ww]ords?\s*:?|Keys words\s*:?|Mots\s?-?\s?[Cc]lés\s?:?'), 1, 0)
        return df

    def remove_elongation(self,df):
        df['level_0'] = df['level_0'].apply(lambda x: re.sub(r'ـ','',x))
        df['level_0'] = df['level_0'].apply(lambda x: re.sub(r'\u200f','',x))
        df['level_0'] = df['level_0'].apply(lambda x: re.sub(r'\(?\*+\)?',' ',x))
        df['level_0'] = df['level_0'].apply(lambda x: re.sub('أ','أ',x))
        df['level_0'] = df['level_0'].apply(lambda x: re.sub('ئ','ئ',x))
        df['level_0'] = np.where(df['level_0'].str.replace('[\d\-%]',"",regex=True).str.strip() == '','',df['level_0'])
        df['level_0'] = np.where(df['level_0'].str.replace('(email:)|(e-mail:)|(E-Mail:)|(Email:)|(البريد الاكتروني)|(البريد الاكتروني:)',"",regex=True).str.strip() == '','',df['level_0'])
        df['level_0'] = np.where((df['level_0'].str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|–').str.split( ).str.len() == 1)&(df['level_0'].str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|–').str.len()<3),'',df['level_0'])
        df = df[df['level_0'].str.strip() != '']
        df['level_0'] = df['level_0'].apply(lambda x: araby.strip_diacritics(x))
        
        return df

    def remove_nb_attached_to_text(self,df):
        df['level_0'] = df['level_0'].apply(lambda x: re.sub(r"(?<!\d|\(|\)|-|:|,|\.)\d{1}(?!\d|\(|\)|-|:|,|\.)",r"",x))
        return df

    def contains_main_keyword(self,df):
        df['contains_main_keywords'] = np.where(df['level_0'].str.contains(r'(كلمات|الكلمات)\s*(المفتاحيّة|المفتاحيّه|الداله|الاصطلاحية|المفتاحيه|المفاتيح|مفاتيج|مفاتيح|الدالّة|مفتاحية|مفتاحيّة|المفتاح|الافتتاحية|مفتاحيه|المفتاحية|الدالة)'),1,0)
        return df

    def contains_date_format(self,df):

        contains_date_format = []
        for a in df['level_0'].to_list():
            x = re.search("([1-9]|0[1-9]|1[0-9]|2[0-9]|3[0-1])\s?(\.|-|/)\s?([1-9]|0[1-9]|1[0-2])\s?(\.|-|/)\s?([0-9][0-9]|19[0-9][0-9]|20[0-9][0-9])$|([0-9][0-9]|19[0-9][0-9]|20[0-9][0-9])\s?(\.|-|/)\s?([1-9]|0[1-9]|1[0-2])\s?(\.|-|/)\s?([1-9]|0[1-9]|1[0-9]|2[0-9]|3[0-1])|(\d{1,2})\s?,?\s?(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s?,?\s+(\d{4})|(\d{1,2})\s?,?\s?(يناير|فبراير|مارس|ابريل|مايو|يونيو|يوليو|أغسطس|سبتمبر|أكتوبر|نوفمبر|ديسمبر)\s?,?\s+(\d{4})|(\d{1,2})\s?,?\s?(كانون الثاني|شباط|آذار|نيسان|أيار|حزيران|تموز|آب|أيلول|تشرين الأول|تشرين الثاني|كانون الأول)\s?,?\s+(\d{4})",a)
            try:
                x.group()
                contains_date_format.append(1)
            except:
                contains_date_format.append(0)
        df['contains_date_format'] = contains_date_format
        return df

    def contains_abstract_keywords(self,df):

        df['contains_main_abstract'] = np.where(df['level_0'].str.contains(r"تلخيص البحث|تلخيص الدراسة|\Aملخص|\Aالملخص|خلاصة البحث|خلاصة الدراسة|\Aمستخلص|\Aالمستخلص|ملخص الدراسة|ملخص البحث|\Aالخلاصة"),1,0)

        df['contains_other_abstract'] = np.where(df['level_0'].str.contains(r'[Aa][Bb]?[Ss][Tt][Rr][Aa][Cc][Tt]|[Ss][Uu][Mm][Mm][Aa][Rr][Yy]|English [Ss]ummary|Résumé'),1,0)

        return df

    def contains_abs_is_single(self,df):
        df['contains_abs_is_single'] = np.where(((df['contains_main_abstract'] == 1)|(df['contains_other_abstract'] == 1)) & df['is_single_word'] == 1,1,0)
        #ind_is_single = df[df['contains_abs_is_single'] == 1].index.to_list()
        #for c in ind_is_single:
         #  single_word = df.loc[c]['level_0']
          # full_abs = df.loc[c+1]['level_0']
          # df.loc[c,'level_0'] = ' '.join([single_word,full_abs])
          # df = df.drop(c+1)

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
#         df['first_itmes'] = np.where(df['Zone']< 5, 1, 0)
        df['first_zone'] = np.where(df['Zone'] == 0, 1, 0)
        df['second_zone'] = np.where(df['Zone'] == 1, 1, 0)

        return df

    def contains_reference_keywords(self,df):

        df['contains_reference_keywords'] = np.where(df['level_0'].str.contains(r'الحواشي|الإحالات|References|الهوامش|المراجع|المصادر'), 1, 0)

        return df

    def starts_with_nb(self,df):

        df['starts_with_nb'] = np.where(df['level_0'].str.contains(r'\A\s?\d'), 1, 0)

        return df

    def contains_url(self,df):

        df['contains_url'] = np.where(df['level_0'].str.contains(r'(https?://[^\s]+)'), 1, 0)

        return df

    def count_aff_keywords(self,df):

        df['count_aff_keywords'] = df['level_0'].str.count(r'معهد|[Un][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy]|جامعة|مخبر دراسات|الجامعة|قسم|كلية|[Dd][Ee][Pp][Aa]?[Rr]?[Tt]?[Mm]?[Ee]?[Nn]?[Tt]?|College')

        return df

    def contains_ara_aff_keywords(self,df):

        df['contains_ara_aff_keywords'] = np.where(df['level_0'].str.contains(r'جامعة|الجامعة|قسم|معهد|كلية|مخبر دراسات|كليه'),1,0)

        return df

    def contains_eng_aff_keywords(self,df):

        df['contains_eng_aff_keywords'] = np.where(df['level_0'].str.contains(r'[Un][Nn][Ii][Vv][Ee][Rr][Ss][Ii][Tt][Yy]|College|[Dd][Ee][Pp][Aa]?[Rr]?[Tt]?[Mm]?[Ee]?[Nn]?[Tt]?'),1,0)

        return df

    def contains_eng_aff_tags(self,df):

        df['contains_eng_aff_tags'] = np.where(df['level_0'].str.contains(r'D[Rr]\s?\.|[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Aa][Ss][Ss][Ii][Ss][Tt]?[Aa]?[Nn]?[Tt]?\.?\s[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Pp]\.?[Hh]\.?[Dd]\.?|Associate [Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?|Masters?\s?o?f?|Researcher|Legal Researcher'),1,0)

        return df

    def contains_ara_aff_tags(self,df):

        df['contains_ara_aff_tags'] = np.where(df['level_0'].str.contains(r'(?<!\w)أ\s?[/\.]|(?<!\w)د\s?[/\.]|أستاذ|أستاذ مساعد|طالب ماجستير|طالب دكتوراة'),1,0)

        return df

    def count_aff_tags(self,df):

        df['count_aff_tags'] = df['level_0'].str.count(r'(?<!\w)أ\s?[/\.]|(?<!\w)د\s?[/\.]|Dr\s?\.|[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Aa][Ss][Ss][Ii][Ss][Tt]?[Aa]?[Nn]?[Tt]?\.?\s[Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?\s?\.|[Pp][Hh][Dd]\.?|أستاذ مساعد|Associate [Pp][Rr][Oo][Ff][Ee]?[Ss]?[Ss]?[Oo]?[Rr]?|Masters?\s?o?f?|Researcher|Legal Researcher')

        return df

    def tables_figures_keywords(self,df):

        df['tables_figures_keywords'] = np.where(df['level_0'].str.contains(r'ا?ل?شكل\s?\(?\d+\)?|ا?ل?جدول\s?\(?\d+\)?|ا?ل?شكل ا?ل?رقم|ا?ل?جدول ا?ل?رقم'),1,0)

        return df

    def count_punctuation(self,df):

        df['count_punctuation'] = df['level_0'].str.count(r'\.|,|،|:|-')

        return df

    def relative_count_punctuation(self,df):

        df['relative_count_punctuation'] = df['level_0'].str.count(r'\.|,|،|-')/df['zone_word_count']
        df['relative_count_punctuation'].fillna(0,inplace = True)

        return df

    def reference_keyword(self,df):

        df['reference_keyword'] = np.where(df['level_0'].str.contains(r'المصادر|المراجع|References|الإحالات|الهوامش'),1,0)

        return df

    def par_year(self,df):

        df['par_year'] = np.where(df['level_0'].str.contains(r'\(\s?\d{4}\s?\)'), 1, 0)

        return df

    def contains_mag_keywords(self,df):
        df['contains_mag_keywords'] = np.where(df['level_0'].str.contains('ا?ل?مجلة|\bا?ل?مجلد|\bا?ل?عدد',regex = True),1,0)

        return df

    def contains_headings(self,df):
        df['contains_headings'] = np.where(df['level_0'].str.strip().replace('(اولآ)|(أولا)|(أوﻻ)|(اوﻻ)|(اولا)|(ثانيا)|(ثالثا)|(رابعا)|(خامسا)|(سادسا)|(سابعا)|(V?I+V?\.)|(V+\.)','',regex=True).str.lstrip('1234567890\s?\.?-? ٓ?~?\(?\)?\/?\s?').str.strip(':|.| |-|/|–|~| ٓ').str.strip().str.contains('^(المقدمة|مقدمة|المقدمة Introduction|تقدمة|تقديم|المقدمة Introduction|المقدمة وأهمية البحث|Introduction|المقدمة وأهمية البحثIntroduction|تمهيد|مشكلة البحث|مشكلة الدراسة|إشكالية الدراسة|إشكالية البحث|اشكالية البحث|اشكالية الدراسة|مشكلات الدراسة|أهمية البحث|أهمية الدراسة|اهمية الدراسة|أهداف الدراسة|اهداف الدراسة|أهداف البحث|هدف الدراسة|هدف البحث|هدفا البحث|هدفا الدراسة|فرضية البحث|فرضية الدراسة|فرضيات البحث|فرضيات الدراسة|التعريف بالبحث|التعريف بالدراسة|منهج البحث|منهج الدراسة|منهجية الدراسة|منهجية البحث|الإطار المنهجي|الإطار المنهجي للبحث|الإطار العام للدراسة|الاطار العام للدراسة|الإطار المنهجي للدراسة|حدود البحث|حدود الدراسة|هيكل الدراسة|هيكل البحث|نموذج الدراسة|نموذج البحث|أنموذج البحث|أنموذج الدراسة|تساؤلات الدراسة|تساؤلات البحث|أسئلة الدراسة|أسئلة البحث|موضوع الدراسة|موضوع البحث|الإطار الزمني|الإطار الزمني للبحث|الإطار الزمني للدراسة|الإطار النظري|الحدود الزمنية|مجال الدراسة و حدودها|مجتمع الدراسة|مجتمع البحث|عينة الدراسة|عينة البحث|مجتمع وعينة الدراسة|أفراد الدراسة|أداة البحث|أداة الدراسة|طريقة جمع العينة|طرق جمع العينات|اساليب جمع البيانات|أساليب جمع البيانات|منهج الدراسة و أدواتها|نتائج الدراسة ومناقشتها|الدراسات السابقة|الدراسات السابقة والمشابهة|أساليب جمع البيانات|النتائج|النتائج Results|نتائج الدراسة|نتائج البحث|النتائج ومناقشتها|النتائج العامة للدراسة|الاستنتاجات|المقترحات|الاستنتاجات والمقترحات|التوصيات|توصيات|توصيات الدراسة|التوصيات والمقترحات|الاستنتاج  Conclusion|Conclusion|الاستنتاج|الخاتمة ونتائج البحث|الخاتمة|خاتمة)\s*[:\n-]?\s*'),1,0)

        df['contains_reference_heading'] = np.where(df['level_0'].str.strip().str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|/|–').str.strip().str.contains('^(المصادر|الإحالات والمراجع|مصادر البحث|المراجع|الهوامش|الهوامش و\s?المراجع|المراجع و\s?الهوامش|الهوامش والمراجع المعتمدة|قائمة المراجع|الهوامش والإحالات|الهوامش والاحالات|مراجع البحث|الهوامش و المراجع المعتمدة|قائمة ببليوجرافية|المصادر والمراجع|ثبت المصادر والمراجع|المراجع والاحالات|الهوامش والمصادر|هوامش البحث|المصادر والمراجع المعتمدة|هوامش وتعليقات البحث|الإحالات والهوامش|الاحالات والهوامش|الإحالات|الاحالات|قائمة المصادر والمراجع|قائمة المراجع والمصادر|قائمة المراجع|مراجع وهوامش البحث|الحواشي والتعليقات|قائمة المصادر|المراجع العربية|المراجع الأجنبية|المراجع الإنجليزية|مراجع وهوامش البحث|مكتبة البحث|الاحالات والمراجع|References|المصادر References|هوامش الدراسة|الإحالات|المصادر والإحالات المعتمدة|المراجع باللّغة العربية|أولا: المراجع العربية|ثانيا: المراجع الأجنبية|المراجع باللّغة الإنجليزية|مراجع باللغة العربية|مراجع باللغة الإنجليزية|الببليوغرافيا|المراجع References|المراجع الاجنبية|المراجع الانجليزية|قائمة المصادروالمراجع)\s*[:\n-]?\s*'),1,0)

        return df

    def in_body(self,df):
        df['in_body'] = 0

        try:
            try:
                first_heading = df[(df['contains_headings'] == 1)].index.values[0]
            except:
                try:
                    first_heading = df[(df['contains_other_keywords'] == 1)].index.values[-1]
                except:
                    first_heading = df[(df['labels'] != 'metadata')].index.values[0]

            try:
                ref_heading = df[(df['contains_reference_heading'] == 1)].index.values[0]
            except:
                ref_heading = df[(df['reference_keyword'] == 1)].index.values[0]


            df.loc[first_heading+1:ref_heading-1,'in_body'] = 1
        except Exception as e:

            pass

        return df

    def contains_metadata_feature(self,df):
        df['contains_metadata_feature'] = np.where((df['contains_main_abstract'] == 1)|(df['contains_other_abstract'] == 1)|(df['contains_abs_is_single'] == 1)|(df['contains_other_keywords'] == 1)|(df['contains_main_keywords'] == 1)|(df['contains_date_keywords'] == 1)|(df['contains_issn_doi'] == 1),1,0)
        return df

    def get_all_features(self):

        pars,tables = self.get_paragraphs_and_tables()
        article_text = [x.get_text().strip() for x in pars if x.get_text().strip() != '']
        article_text = ' '.join([x for x in article_text])

        selectors = self.get_selectors_dict()

        df = self.extract_features_from_par(pars,selectors)

        df = self.prep_font_family(df)
        
        df = self.prep_lang_feature(df)
        
        df = self.prep_font_style(df)
        
        df = self.prep_bold_feature(df)
        
        df = self.get_word_counts(df)

        df = self.word_counts_features(df)
        
        df = self.prep_direction_feature(df)

        df = self.prep_font_size(df)

        df = self.prep_footnote_feature(df)

        df = self.prep_align_feature(df)

        df = self.drop_unwanted_features(df)

        df = self.prep_indetation(df)

        df = df.groupby(['zone']).agg(lambda x:x.value_counts().index[0])
        
        df = self.remove_elongation(df)
        
        df = self.is_first_page(df)

        df = self.single_word_feature(df)

        df = self.contains_abstract_keywords(df)

        df = self.contains_abs_is_single(df)

        df = self.remove_nb_attached_to_text(df)

#         df = self.elongated_words_feature(df)

        df = self.add_zone_lang(df)

        df = self.contains_names(df)

        df = self.contains_aff(df)

        df = self.contains_emails(df)

        df = self.add_is_max_font(df)

        df = self.contains_other_keyword(df)

        df = self.contains_main_keyword(df)

        df = self.contains_date_format(df)

        df = self.contains_date_keywords(df)

        df = self.contains_issn_doi(df)

#         df = self.contains_reference_keywords(df)

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

        df = self.reference_keyword(df)

        df = self.par_year(df)

        df = self.contains_mag_keywords(df)

        df = self.contains_headings(df)

        df = self.in_body(df)

        df = self.contains_metadata_feature(df)

        df = df[df['level_0'].str.strip() != '']

        df['sub_label'] = np.nan
        df['labels'] = np.nan

#         df['path_to_txt'] = 'docx_to_txt/'+self.path_to_html.split('/')[1].split('.')[0]+'.txt'

        return df,article_text
