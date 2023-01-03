import xml.etree.ElementTree as ET
import xmltodict
import itertools
import re
import os
import pandas as pd
import unicodedata

class ExtractLabels():
    
    def __init__(self,path_to_labels_xml):
        
        self.path_to_labels_xml = path_to_labels_xml
        
    def get_labels(self):
        text_and_labels = []

        path_to_txt = 'docx_to_txt/'+self.path_to_labels_xml.split('/')[1].split('.aif')[0]

        tree = ET.parse(self.path_to_labels_xml)
        xml_data = tree.getroot()
        #here you can change the encoding type to be able to set it to the one you need
        xmlstr = ET.tostring(xml_data, encoding='utf-8', method='xml')

        data_dict = dict(xmltodict.parse(xmlstr))

        data_dict = data_dict['ns0:Corpus']

        Regions = data_dict['ns0:RegionSet']['ns0:Region']
        regions = {}
        for d in Regions:
            regions[d['@id']] = dict(itertools.islice(d.items(), 1,len(d.items())))

        Anchors = data_dict['ns0:AnchorSet']['ns0:Anchor']
        anchors = {}
        for d in Anchors:
            anchors[d['@id']] = dict(itertools.islice(d.items(), 2,len(d.items())-1))

        #get annotation sets
        AnnotationSet = data_dict['ns0:Analysis']['ns0:AnnotationSet']
        #there are four sets
        a = 0
        for annset in AnnotationSet:
            i=0
            try:
                annset['ns0:Annotation']['ns0:RegionRef']
                zone = annset['ns0:Annotation']

                text_and_labels.append(self.get_text(zone,regions,anchors,path_to_txt))
                i+=1
            except Exception as e:
                try:
                    for zone in annset['ns0:Annotation']:

                        text_and_labels.append(self.get_text(zone,regions,anchors,path_to_txt))
                        i+=1
                except Exception as e:
#                     print(path_to_html)
#                     print(e)
#                     print(a)
#                     print(i)
#                     print('no labels for this annset')
                    i+=1
                    pass
            a += 1
            
        labels_df = pd.DataFrame(text_and_labels)
        labels_df = labels_df.rename({0:'text',1:'lang',2:'sub_label',3:'main_label',4:'path_to_txt',5:"order"},axis=1)
        labels_df['text'] = labels_df['text'].apply(lambda x: unicodedata.normalize("NFKD", x))
        labels_df = labels_df.sort_values('order',ignore_index=True)
        
        return labels_df
    
    def get_text(self,zone,regions,anchors,path_to_txt):
    
        region = zone['ns0:RegionRef']['@ns1:href'].split('#')[1]

        reg_nb = re.compile('\d+').findall(region)[0]

        target_lables = regions[region]['ns0:AnchorRef']

        end_target = target_lables[0]['@ns1:href'].split('#')[1]
        end_char = int(anchors[end_target]['ns0:Parameter']['#text'])

        start_target = target_lables[1]['@ns1:href'].split('#')[1]
        start_char = int(anchors[start_target]['ns0:Parameter']['#text'])

        with open(path_to_txt) as f:
            all_text = f.read()
        target_text = all_text[start_char:end_char]  
        target_text = re.sub(r'\n',r' ',target_text)
        target_text = re.sub(r'\|',r'',target_text)
        target_text = re.sub(r'\s{2,}',r" ",target_text)
        target_text = target_text.strip()

        main_label = zone['@type']
        if main_label == 'metadata':
            lang = zone['ns0:Content']['ns0:Parameter'][0]['#text']
            specific_label = zone['ns0:Content']['ns0:Parameter'][1]['#text']
        elif main_label == 'references':
            lang = zone['ns0:Content']['ns0:Parameter']['#text']
            specific_label = 'references'
        else:
            lang = 'arabic'
            specific_label = zone['ns0:Content']['ns0:Parameter']['#text']

        return [target_text,lang,specific_label,main_label,path_to_txt,reg_nb]
