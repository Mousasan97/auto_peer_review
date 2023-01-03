# auto_peer_review
This project is for aiding in the peer-review process by detecting the presence/absence of essential components and headings.
The task is done through two models: the first one classifies text into **metadata**, **body**, **references**, or **others**, 
and the second model takes the results from the first model that are classified as **metadata** and further classifies them into sub_labels.
The other results, **body**, **references**, and **others**, are cleaned and categorised using emperical methods and regular expression.
The models can only process **docx** files. 
**docx** files are converted to html to be able to extract the text with its styling.
The styling of the text is used as features, and new features are also extracted from the text to be inputted to the models.

### Papers used as reference for project
[CERMINE](https://www.researchgate.net/publication/265469056_CERMINE_--_Automatic_Extraction_of_Metadata_and_References_from_Scientific_Literature)

[Automatic extraction of metadata from scientific publications for CRIS systems](https://www.researchgate.net/publication/216592386_Automatic_extraction_of_metadata_from_scientific_publications_for_CRIS_systems)

### Description of folders and scripts
#### Main scripts:

**Extracting_Features_html_new.py** is the script that process the html file. It extracts the text and the styling, 
it preprocesses the text, it creates features from the styling and creates new features from the text.

**run_models.py** it reads the docx file, converts it to html, calls *Extracting_Features_html_new.py*, inserts features into the models and returns the labels.

**run_auto_peer.py** is the main script used to run the program. It calls *run_models.py* and takes the results and preps them to be inderted to marefa in the correct format with the text.
The script takes as input the path to the docx file to be processed:
```
 python -m run_auto_peer "0 (4).docx"
 python -m run_auto_peer <"path_to_file>
```

#### Other files and folders:
- **data** has all data files used to assist the scripts and models
    - **all_ara_names.txt** list of arabic names
    - **all_eng_names.txt** list of eng names
    - **ara_aff.txt** list of affiliations (universities, institutes, etc..) in arabic
    - **eng_aff.txt** list of affiliations (universities, institutes, etc..) in english
    - **full_dataset7.csv** table of text and features used to train the models
    - **list_of_citites.txt** list of cities in arabic and english
    - **list_of_countries.txt** list of countries in arabic and english
- **models** contains the two models used
    - **first_model.sav** model used to classify text into **metadata**, **body**, **references**, or **others**
    - **metadata_model.sav** model used to classify metadata into sub_labels
- **scalers**
    - **first_model_scaler.sav** 
    - **metadata_model_scaler.sav**
- **Extracting_Features_html.py** used to extract text and styling from previous html format (not used anymore) 
##### **Scripts used to prep data pre-training**
  - **Extract_Labels.py** used to attach labels created using [Callisto](https://mitre.github.io/callisto/manual/install.html#Annotation_Tasks) to text from docx.
  - **Join_Features_Labels.py** used to join the labels to features extracted from html.
