import pandas as pd
import re
import numpy as np
class JoinFeaturesLabels():
    
    def __init__(self,features_df,labels_df):
        
        self.features_df = features_df
        self.features_df['level_0'] = self.features_df['level_0'].apply(lambda x: re.sub('أ','أ',x))
        self.features_df['level_0'] = self.features_df['level_0'].apply(lambda x: re.sub('ئ','ئ',x))
        self.features_df['level_0'] =self.features_df['level_0'].apply(lambda x: re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',x))
                                                                        
        self.labels_df = labels_df
        self.labels_df['text'] = self.labels_df['text'].apply(lambda x: re.sub('أ','أ',x))
        self.labels_df['text'] = self.labels_df['text'].apply(lambda x: re.sub('ئ','ئ',x))
        self.labels_df['text'] = self.labels_df['text'].apply(lambda x: re.sub('(?<=\s[ا-ي]{1}) (?=\w+)|\uf0a7','',x))
                                                                        
    # Function to insert row in the dataframe
    def insert_row(self,row_number, df, row_value):
        # Starting value of upper half
#         start_upper = 0

#         # End value of upper half
#         end_upper = row_number

#         # Start value of lower half
#         start_lower = row_number

#         # End value of lower half
#         end_lower = df.shape[0]

#         # Create a list of upper_half index
#         upper_half = [*range(start_upper, end_upper, 1)]

#         # Create a list of lower_half index
#         lower_half = [*range(start_lower, end_lower, 1)]

#         # Increment the value of lower half by 1
#         lower_half = [x.__add__(1) for x in lower_half]
        
        upper_half = df.loc[:row_number-1].index.values.tolist()

        # Create a list of lower_half index
        lower_half = df.loc[row_number:].index.values.tolist()

        # Increment the value of lower half by 1
        lower_half = [x.__add__(1) for x in lower_half]
    
        # Combine the two lists
        index_ = upper_half + lower_half

        # Update the index of the dataframe
        df.index = index_

        # Insert a row at the end
        df = df.append(row_value,ignore_index=False)

        # Sort the index labels
        df = df.sort_index()

        # return the dataframe
        return df
        
    def join(self):
        
        matches = {}
        matched_labels = []
        matched_sub_labels = []
        i=0
    
        for t in self.features_df['level_0'].to_list():
            try:

                matched = [a for a in self.labels_df['text'].to_list() if re.sub('\s{2,}',' ', re.sub('[,،.]','',t.strip())) in re.sub('\s{2,}',' ', re.sub('[,،.]','',a.strip()))]
                if len(matched) == 1:
                    
                    self.features_df.iloc[i,self.features_df.columns.get_loc('sub_label')] = self.labels_df[self.labels_df['text'] == matched[0]]['sub_label']
                    self.features_df.iloc[i,self.features_df.columns.get_loc('labels')] = self.labels_df[self.labels_df['text'] == matched[0]]['main_label']
                    
                    matched_labels.append(self.labels_df[self.labels_df['text'] == matched[0]].index[0])
                elif len(matched) > 1:
                    matches[t] = matched
                    inds = self.labels_df[self.labels_df['text'].isin(matched)].index.to_list()
                    
                    nearest_ind = len(self.features_df)+10
                    min_dist = len(self.features_df)+10
                    for ind in inds:
                        if abs(i-ind) < min_dist:
                            min_dist = abs(i-ind)
                            nearest_ind = ind
                       
                    self.features_df.iloc[i,self.features_df.columns.get_loc('sub_label')]= self.labels_df.iloc[nearest_ind]['sub_label']
                    self.features_df.iloc[i,self.features_df.columns.get_loc('labels')]= self.labels_df.iloc[nearest_ind]['main_label']  
                    matched_labels.append(nearest_ind)
#                     print('matched {}'.format(nearest_ind))

            except:
                pass
            i+=1
            
        unmatched_labels = [a for a in self.labels_df.index.to_list() if a not in set(matched_labels)]
        
        if len(unmatched_labels) >0:
            new_rows = []
            newly_matched = []
            i = 0
            for t in self.labels_df.iloc[unmatched_labels]['text'].to_list():
                matched = [a for a in self.features_df[self.features_df['labels'].isnull()]['level_0'].to_list() if re.sub('\s{2,}',' ', re.sub('[,،.]','',t.strip())) in re.sub('\s{2,}',' ', re.sub('[,،.]','',a.strip()))]
                
                if len(matched) > 0:
                    row_to_add = self.features_df[self.features_df['labels'].isnull()][self.features_df['level_0'] == matched[0]]
                    newly_matched.append(matched[0])
                    index_to_add = row_to_add.index.values[0]
                    
                    row_to_add['level_0'] = t
                    row_to_add['labels'] = self.labels_df.iloc[unmatched_labels].iloc[i]['main_label']
                    row_to_add['sub_label'] = self.labels_df.iloc[unmatched_labels].iloc[i]['sub_label']
                    
                    new_rows.append(row_to_add)
                    self.features_df = self.insert_row(index_to_add,self.features_df,row_to_add)
                    
#                     self.features_df = self.features_df.append(row_to_add,ignore_index=True)
                    
#                 matched_sub = [a for a in self.features_df[self.features_df['sub_label'].isnull()]['level_0'].to_list() if t.strip() in a.strip()]
#                 if len(matched_sub) > 0:
#                     row_to_add = self.features_df[self.features_df['sub_label'].isnull()][self.features_df['level_0'] == matched[0]]
#                     row_to_add['level_0'] = t
#                     row_to_add['sub_label'] = self.labels_df.iloc[unmatched_labels].iloc[i]['sub_label']
#                     new_rows.append(row_to_add)
#                     self.features_df = self.features_df.append(row_to_add,ignore_index=True)
                i+=1
            if len(newly_matched)>0:
                newly_matched = list(set(newly_matched))
                self.features_df = self.features_df[~self.features_df['level_0'].isin(newly_matched)]
                
        self.features_df = self.features_df[~self.features_df['labels'].isnull()]
#         self.features_df = self.features_df.sort_values(['zone'])
        
#         labels = self.features_df['labels'].to_list()
#         features = self.features_df.drop(['level_0','labels'],axis=1)
#         self.features_df = self.features_df.drop(['level_0'],axis=1)
    
        return self.features_df