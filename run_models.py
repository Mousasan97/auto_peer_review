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

import Extracting_Features_html_new as E_F
import Extract_Labels as E_L
import Join_Features_Labels as J_F_L
import os
import subprocess
import collections
import pandas as pd
import copy
import numpy as np
import textract

from sklearn import metrics
from sklearn.svm import SVC
from sklearn.model_selection import cross_val_score, GridSearchCV,train_test_split,KFold,StratifiedKFold
from sklearn.preprocessing import StandardScaler,LabelEncoder
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline

import warnings
warnings.filterwarnings("ignore")

margin_headings = ['الهوامش','هوامش البحث','هوامش وتعليقات البحث','الحواشي والتعليقات','هوامش الدراسة','الهوامش والاشارات','الإحالات','الاحالات','الهوامش والإحالات','الهوامش والاحالات']
reference_headings = ['المصادر','مصادر البحث','الإحالات والمراجع','المراجع','الهوامش والمراجع','مراجع البحث','المراجع والهوامش','الهوامش والمراجع المعتمدة','الهوامش و المراجع المعتمدة','المصادر والمراجع','ثبت المصادر والمراجع','المراجع والاحالات','الهوامش والمصادر','المصادر والمراجع المعتمدة','قائمة المصادر والمراجع','قائمة المراجع',"قائمة المراجع",'مراجع وهوامش البحث','قائمة المصادر','قائمة المصادر','مراجع وهوامش البحث','مكتبة البحث','الاحالات والمراجع','References','المصادر References','الإحالات','المصادر والإحالات المعتمدة','الببليوغرافيا','المراجع References','قائمة المراجع والمصادر' ,'قائمة ببليوجرافية','الإحالات والهوامش','الاحالات والهوامش','قائمة المصادروالمراجع','المراجع العربية','قائمة المراجع باللغة العربية','قائمة المراجع باللغة الإنجليزية','قائمة المراجع باللغة الأجنبية','المراجع الأجنبية','المراجع الأجنبية','المراجع الإنجليزية','المراجع باللّغة العربية','المراجع باللّغة الإنجليزية','مراجع باللغة العربية','مراجع باللغة الإنجليزية','المراجع الاجنبية',"أولاً: المراجع العربية","أولا: المراجع العربية","ثانياً: المراجع الأجنبية",'ثانيا: المراجع الأجنبية','المراجع الانجليزية','المراجع باللغة العربية','المراجع باللغة الأجنبية']

class Structure():

    def __init__(self,path_first_model,path_metadata_model,
                 path_first_scaler,path_metadata_scaler,path_file,path_all_dfs):

        with open(path_first_model, "rb") as f:
            self.first_model = pickle.load(f)

        with open(path_metadata_model, "rb") as f:
            self.metadata_model = pickle.load(f)

        with open(path_first_scaler, "rb") as f:
            self.first_scaler = pickle.load(f)

        with open(path_metadata_scaler, "rb") as f:
            self.metadata_scaler = pickle.load(f)

        self.all_dfs_concat = pd.read_csv(path_all_dfs,sep='\t')
        self.path_file = path_file
#         self.path_html = path_file.split('/')[1].split('.doc')[0]+'.html'
#         self.path_html = path_file.split('.doc')[0]+'.html'

    def convert_doc_to_docx(self):
        print('converting doc to docx')
        subprocess.call(['lowriter', '--headless', '--convert-to', 'docx', self.path_file])

    def save_file_as_html(self,file):

#         subprocess.run(['soffice', '--headless', '--convert-to', 'html:XHTML Writer File:UTF8',file], capture_output=True)
        subprocess.run(['soffice', '--headless', '--convert-to', 'html:XHTML Writer File:UTF8','--outdir', '/home/ubuntu/peer_files', file], capture_output=True)

    def load_file(self,path_html):

        EF = E_F.ExtractFeatures(path_html,'data/all_ara_names.txt',
                                 'data/all_eng_names.txt',
                                 'data/ara_aff.txt',
                                 'data/eng_aff.txt')

        features_df,all_text = EF.get_all_features()

        if 'is_footnote' in features_df.columns:
            values_to_add =[0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0]

            features_df.loc[features_df.index[0],['text-align_b','font-size_b','font-family_b',"direction_b",
                               'zone_word_count_b','text-indent_b',
                               'contains_ara_authors_b','contains_eng_authors_b',
                               'contains_email_b','contains_ara_aff_b',
                               'contains_eng_aff_b','is_single_word_b',
                               'is_footnote_b','is_max_font_b',
                               'contains_other_keywords_b','contains_main_keywords_b',
                               'contains_date_format_b','contains_main_abstract_b',
                               'contains_other_abstract_b','contains_date_keywords_b',
                               'starts_with_nb_b',
                               'contains_url_b','contains_ara_aff_keywords_b',
                               'contains_eng_aff_keywords_b','count_aff_keywords_b',
                               'contains_eng_aff_tags_b','contains_ara_aff_tags_b',
                               'count_aff_tags_b','tables_figures_keywords_b','relative_count_punctuation_b',
                               'contains_abs_is_single_b','reference_keyword_b','contains_mag_keywords_b',
                               'contains_headings_b','contains_reference_heading_b',
                               'perc_eng_aff_b','perc_ara_aff_b']] = values_to_add
        else:
            values_to_add =[0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0]

            features_df.loc[features_df.index[0],['text-align_b','font-size_b','font-family_b',"direction_b",
                               'zone_word_count_b','text-indent_b',
                               'contains_ara_authors_b','contains_eng_authors_b',
                               'contains_email_b','contains_ara_aff_b',
                               'contains_eng_aff_b','is_single_word_b',
                               'is_max_font_b','contains_other_keywords_b',
                               'contains_main_keywords_b','contains_date_format_b',
                               'contains_main_abstract_b','contains_other_abstract_b',
                               'contains_date_keywords_b','starts_with_nb_b','contains_url_b',
                               'contains_ara_aff_keywords_b','contains_eng_aff_keywords_b',
                               'count_aff_keywords_b','contains_eng_aff_tags_b','contains_ara_aff_tags_b',
                               'count_aff_tags_b','tables_figures_keywords_b','relative_count_punctuation_b',
                               'contains_abs_is_single_b','reference_keyword_b','contains_mag_keywords_b',
                               'contains_headings_b','contains_reference_heading_b',
                               'perc_eng_aff_b','perc_ara_aff_b']] = values_to_add

        for i in range(1,len(features_df)):

            if 'is_footnote' in features_df.columns:
                try:
                    values_to_add = features_df.iloc[i-1][['text-align','font-size','font-family','direction',
                                   'zone_word_count','text-indent',
                                   'contains_ara_authors','contains_eng_authors',
                                   'contains_email','contains_ara_aff',
                                   'contains_eng_aff','is_single_word',
                                   'is_footnote','is_max_font',
                                   'contains_other_keywords','contains_main_keywords',
                                   'contains_date_format','contains_main_abstract',
                                   'contains_other_abstract','contains_date_keywords',
                                   'starts_with_nb',
                                   'contains_url','contains_ara_aff_keywords',
                                   'contains_eng_aff_keywords','count_aff_keywords',
                                   'contains_eng_aff_tags','contains_ara_aff_tags',
                                   'count_aff_tags','tables_figures_keywords','relative_count_punctuation',
                                   'contains_abs_is_single','reference_keyword','contains_mag_keywords',
                                   'contains_headings','contains_reference_heading','in_body',
                                   'perc_eng_aff','perc_ara_aff']].copy(deep=True).values
                except Exception as e:
                    print(e)

                features_df.loc[features_df.index[i],['text-align_b','font-size_b','font-family_b',"direction_b",
                               'zone_word_count_b','text-indent_b',
                               'contains_ara_authors_b','contains_eng_authors_b',
                               'contains_email_b','contains_ara_aff_b',
                               'contains_eng_aff_b','is_single_word_b',
                               'is_footnote_b','is_max_font_b',
                               'contains_other_keywords_b','contains_main_keywords_b',
                               'contains_date_format_b','contains_main_abstract_b',
                               'contains_other_abstract_b','contains_date_keywords_b',
                                                      'starts_with_nb_b',
                               'contains_url_b','contains_ara_aff_keywords_b',
                               'contains_eng_aff_keywords_b','count_aff_keywords_b',
                               'contains_eng_aff_tags_b','contains_ara_aff_tags_b',
                               'count_aff_tags_b','tables_figures_keywords_b','relative_count_punctuation_b',
                               'contains_abs_is_single_b','reference_keyword_b','contains_mag_keywords_b',
                               'contains_headings_b','contains_reference_heading_b','in_body_b',
                               'perc_eng_aff_b','perc_ara_aff_b']] = values_to_add
            else:
                values_to_add = features_df.iloc[i-1][['text-align','font-size','font-family','direction',
                               'zone_word_count','text-indent',
                               'contains_ara_authors','contains_eng_authors',
                               'contains_email','contains_ara_aff',
                               'contains_eng_aff','is_single_word',
                               'is_max_font','contains_other_keywords',
                               'contains_main_keywords','contains_date_format',
                               'contains_main_abstract','contains_other_abstract',
                               'contains_date_keywords','starts_with_nb','contains_url','contains_ara_aff_keywords',
                               'contains_eng_aff_keywords','count_aff_keywords',
                               'contains_eng_aff_tags','contains_ara_aff_tags',
                               'count_aff_tags','tables_figures_keywords','relative_count_punctuation',
                               'contains_abs_is_single','reference_keyword','contains_mag_keywords',
                               'contains_headings','contains_reference_heading','in_body',
                               'perc_eng_aff','perc_ara_aff']].copy(deep=True).values

                features_df.loc[features_df.index[i],['text-align_b','font-size_b','font-family_b',"direction_b",
                               'zone_word_count_b','text-indent_b',
                               'contains_ara_authors_b','contains_eng_authors_b',
                               'contains_email_b','contains_ara_aff_b',
                               'contains_eng_aff_b','is_single_word_b',
                               'is_max_font_b','contains_other_keywords_b',
                               'contains_main_keywords_b','contains_date_format_b',
                               'contains_main_abstract_b','contains_other_abstract_b',
                               'contains_date_keywords_b','starts_with_nb_b','contains_url_b','contains_ara_aff_keywords_b',
                               'contains_eng_aff_keywords_b','count_aff_keywords_b',
                               'contains_eng_aff_tags_b','contains_ara_aff_tags_b',
                               'count_aff_tags_b','tables_figures_keywords_b','relative_count_punctuation_b',
                               'contains_abs_is_single_b','reference_keyword_b','contains_mag_keywords_b',
                               'contains_headings_b','contains_reference_heading_b','in_body_b',
                               'perc_eng_aff_b','perc_ara_aff_b']] = values_to_add

#         features_df['face'] = features_df['face'].str.replace(r"[^a-zA-Z\s]",'',regex = True)
#         features_df['face'] = features_df['face'].str.strip()
#         features_df['face'] = features_df['face'].replace('',np.nan)
#         features_df['face'].fillna(features_df['face'].value_counts().index[0],inplace = True)
#         features_df['face_b'] = features_df['face_b'].str.replace(r"[^a-zA-Z\s]",'',regex = True)
#         features_df['face_b'] = features_df['face_b'].str.strip()
#         features_df['face_b'] = features_df['face_b'].replace('',np.nan)
#         features_df['face_b'].fillna(features_df['face_b'].value_counts().index[0],inplace = True)

#         features_df = pd.get_dummies(features_df, columns = ['align','face','dir','align_b','face_b',
#                                                             'dir_b'])



        features_df['text-indent'] = features_df['text-indent'].astype(str).apply(lambda x: x.split('in')[0].strip())
        features_df['text-indent_b'] = features_df['text-indent'].astype(str).apply(lambda x: x.split('in')[0].strip())
        features_df = pd.get_dummies(features_df, columns = ['text-align','font-family','direction','text-align_b','font-family_b','direction_b'])

        features_df = features_df.reindex(self.all_dfs_concat.columns, axis=1)
        features_df['is_footnote'].fillna(0,inplace = True)
        features_df['is_footnote_b'].fillna(0,inplace = True)

        features_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        features_df = features_df.fillna(int(0))
        features_df = features_df.reset_index(drop=True)

        return features_df,all_text

    def fit_models(self):

        if self.path_file.endswith('.doc'):
            self.convert_doc_to_docx()
            fn = self.path_file.split('/')[-1].split('.doc')[0]+'.docx'
        else:
            fn = self.path_file
        print(fn)
        try:
#             path_html = fn.split('/')[-1].split('.doc')[0]+'.html'
            path_html = '/home/ubuntu/peer_files/'+ fn.split('/')[-1].split('.doc')[0]+'.html'
        except:
#             path_html = fn.split('.doc')[0]+'.html'
            path_html = '/home/ubuntu/peer_files/'+fn.split('.doc')[0]+'.html'

        try:
            self.save_file_as_html(fn)
        except Exception as e:
            print(e)

        features_df,all_text = self.load_file(path_html)

        features = features_df.drop(['level_0','sub_label','labels','sub_label_b_acknowledgment',
                                   'sub_label_b_bib_info',
                                   'sub_label_b_copyright','sub_label_b_correspondence',
                                   'sub_label_b_dates', 'sub_label_b_equation', 'sub_label_b_figure',
                                   'sub_label_b_figure_heading','sub_label_b_main_abstract',
                                   'sub_label_b_main_affiliation','sub_label_b_main_authors',
                                   'sub_label_b_main_keywords','sub_label_b_main_title',
                                   'sub_label_b_other_abstract','sub_label_b_other_affiliation',
                                   'sub_label_b_other_authors','sub_label_b_other_keywords',
                                   'sub_label_b_other_title','sub_label_b_page_number',
                                   'sub_label_b_references','sub_label_b_table',
                                   'sub_label_b_table_heading','sub_label_b_text',
                                   'sub_label_b_unknown',
                                     'starts_with_nb','starts_with_nb_b'],axis=1)
#         print(features.columns[:60])
#         print(features.columns[60:120])
#         print(features.columns[120:180])
#         print(features.columns[182:])

        main_labels = []
        feature_std = self.first_scaler.transform(features.iloc[0].values.reshape(1, -1))
        pred = self.first_model.predict(feature_std)
        main_labels.append(pred[0])
        prev_abs = False
        
        for i in range(1,len(features)):
            
            features.iloc[i,features.columns.get_loc('labels_b_'+pred[0])] = 1
            features_df.iloc[i,features_df.columns.get_loc('labels_b_'+pred[0])] = 1
            
            feature_std = self.first_scaler.transform(features.iloc[i].values.reshape(1, -1))
                
            pred = self.first_model.predict(feature_std)
            if prev_abs and pred[0] != 'metadata':
                main_labels.append('metadata')
                prev_abs = False
            else:
                main_labels.append(pred[0])
                
            if features.iloc[i]['contains_abs_is_single'] == 1:
                 prev_abs = True
                    
        features_df['label'] = main_labels
        
        app_ind = features_df[(features_df['level_0'].str.contains('ملحق رقم|الملحق\s?\(?\d+\)?|الملاحق',regex=True))&(features_df['level_0'].str.split( ).str.len()<4)].index.to_list()
        is_ref_heading = features_df[features_df['level_0'].str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|–').isin(reference_headings+margin_headings)].index.to_list()
        
        if app_ind and is_ref_heading and (is_ref_heading[0]>app_ind[0]):
            features_df = pd.concat([features_df.loc[:app_ind[0]-1],features_df.loc[is_ref_heading[0]:]])
        elif app_ind:
            features_df = features_df.loc[:app_ind[0]-1]
            
       # metadata_features_df = features_df[features_df['label'] == 'metadata']
        metadata_features_df = features_df[((features_df['label'] == 'metadata')|((features_df['contains_metadata_feature'] ==1)&(features_df['is_first_page'] ==1))|((features_df['contains_ara_authors'] ==1)|(features_df['contains_eng_authors'] ==1))&(features_df['is_first_page'] ==1)|((features_df['contains_metadata_feature'] ==1)&(features_df['contains_other_abstract'] ==1)))&(features_df['label'] != 'references')]
        
        metadata_features = metadata_features_df.drop(['sub_label','labels','level_0','label',
                                         'labels_b_body','labels_b_metadata','labels_b_other',
                                        'labels_b_references',
                                       'sub_label_b_acknowledgment','sub_label_b_copyright',
                                       'sub_label_b_equation', 'sub_label_b_figure',
                                       'sub_label_b_figure_heading','sub_label_b_page_number',
                                       'sub_label_b_references','sub_label_b_table',
                                       'sub_label_b_table_heading','sub_label_b_text',
                                        'sub_label_b_unknown',
                                     'starts_with_nb','starts_with_nb_b'],axis=1)
#         print(metadata_features.columns[:60])
#         print(metadata_features.columns[60:120])
#         print(metadata_features.columns[120:180])
#         print(metadata_features.columns[182:])
        sub_labels = []
        feature_std = self.metadata_scaler.transform(metadata_features.iloc[0].values.reshape(1, -1))
        pred = self.metadata_model.predict(feature_std)
        sub_labels.append(pred[0])

        for i in range(1,len(metadata_features)):

            metadata_features.iloc[i,metadata_features.columns.get_loc('sub_label_b_'+pred[0])] = 1
            feature_std = self.metadata_scaler.transform(metadata_features.iloc[i].values.reshape(1, -1))

            pred = self.metadata_model.predict(feature_std)
            sub_labels.append(pred[0])

        metadata_features_df['sub_label'] = sub_labels
        metadata_features_df = metadata_features_df[~((metadata_features_df['sub_label'] == 'main_affiliation') & (metadata_features_df['perc_ara_aff'] == 0))]
        
        if len(features_df[features_df['contains_main_abstract'] ==1])==0 and (len(metadata_features_df[metadata_features_df['sub_label'] =='main_abstract']))>2:
            metadata_features_df = metadata_features_df[metadata_features_df['sub_label'] !='main_abstract']
                                                                                                

        is_ref_heading = features_df[features_df['level_0'].str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|–').isin(reference_headings)].index.to_list()
        is_marg_heading = features_df[features_df['level_0'].str.lstrip('1234567890\s?\.?-?\(?\)?\/?\s?').str.strip(':|.| |-|–').isin(margin_headings)].index.to_list()
        if is_marg_heading and not is_ref_heading:
            ref_headings_ind = is_marg_heading
        else:
            ref_headings_ind = is_ref_heading
        if len(ref_headings_ind) > 0:
            if len(ref_headings_ind) > 1:
                if (ref_headings_ind[-2] +1) != ref_headings_ind[-1]:
                    ind = ref_headings_ind[0]
                else:
                    ind = ref_headings_ind[-1]
            else:
                ind = ref_headings_ind[0]

            references_df = features_df.loc[ind:]
            if len(references_df) > 2:
                references_df = references_df[references_df['label'] != 'metadata']
                references_df = references_df[references_df['label'] != 'other']
            #references_df = references_df[~references_df['level_0'].str.lstrip('1234567890\s?\.?\(?\)?\s?').strip(':|.| |-').isin(reference_headings)]
        else:
            ref_headings_ind = features_df[features_df['contains_reference_heading'] ==1].index.to_list()
            if len(ref_headings_ind) > 0 and len(features_df[features_df['contains_headings'] ==1]) <0:
                if len(ref_headings_ind) > 1:
                    if (ref_headings_ind[-2] +1) != ref_headings_ind[-1]:
                        ind = ref_headings_ind[0]
                    else:
                        ind = ref_headings_ind[-1]
                else:
                    ind = ref_headings_ind[0]

                references_df = features_df.loc[ind+1:]
                metadata_features_df = metadata_features_df.loc[~metadata_features_df.index.isin(references_df.index)]
            else:
                references_df = features_df[features_df['label'] == 'references']
                
        if len(references_df.iloc[1:][references_df.iloc[1:]['is_footnote'] =='1']) > 0 and len(references_df.iloc[1:][references_df.iloc[1:]['is_footnote'] =='1']) != len(references_df.iloc[1:]):
            len_ftn = len(references_df[references_df['is_footnote'] =='1']) -1
            if len_ftn >0 and len(references_df[-len_ftn:][references_df['is_footnote'] == '1']) == len_ftn:
                references_df = references_df[(references_df.index == references_df.index[0]) | (references_df['is_footnote'] !='1')]
            elif len_ftn == 0 and len(references_df[-1:][references_df['is_footnote'] == '1']) == 1:
                references_df = references_df[(references_df.index == references_df.index[0]) | (references_df['is_footnote'] !='1')]
            elif len(references_df[references_df['is_footnote'] =='1']) >2 and len(references_df[-2:][references_df['is_footnote'] == '1']) == 2:
                references_df =references_df[(references_df.index==references_df.index[0])|(references_df['is_footnote'] !='1')]


        headings = features_df[(features_df['contains_headings'] ==1)]
        headings['level_0'] = headings['level_0'].str.strip().replace('(اولآ)|(أولا)|(أوﻻ)|(اوﻻ)|(اولا)|(ثانيا)|(ثالثا)|(رابعا)|(خامسا)|(سادسا)|(سابعا)|(V?I+V?\.)|(V+\.)','',regex=True).str.lstrip('1234567890\s?\.?-?–? ٓ?~?\(?\)?\/?\s?').apply(lambda x:x.split(':')[0]).str.strip(':|.| |-|/|–|~| ٓ').str.strip()
#         os.remove(path_html)
        return metadata_features_df[['level_0','sub_label']],references_df,headings,all_text
