


# Title
> *Your title should be no longer than 200 characters. Avoid acronyms and abbreviations where possible. Also avoid leading with "The". Only letters, numbers, spaces, underscores, and hyphens are allowed.*
> - *If your dataset is derived from MIMIC and you would like to use the MIMIC acronym, please include the letters "Ext" (for example, MIMIC-IV-Ext-YOUR-DATASET"). Ext may either indicate "extracted" (e.g. a derived subset) or "extended" (e.g. annotations), depending on your use case.*
> - *If the dataset is derived from another dataset, the title must make this clear.*


MIMIC-IV-Ext-CLIF


# Abstract
> *A brief description of the resource and the context in which it was created.*
> 
> *Your abstract must be no longer than 250 words. The focus should be on the resource being shared. If the resource was generated as part of a scientific investigation, relevant information may be provided to facilitate reuse. References should not be included. The abstract should also include a high-level description of the data as well as an overview of the key aims of the project. The abstract may appear in search indexes independently of the full project metadata, so providing detailed information about the content is important.*



# Background
> *Your background should provide the reader with an introduction to the resource. The section should offer context in which the resource was created and outline your motivations for sharing.*

## Why CLIF is useful
Critical illness, or acute organ failure requiring life support, threatens over five million American lives annually. Electronic health record (EHR) data are a source of granular information that could generate crucial insights into the nature and optimal treatment of critical illness. However, data management, security, and standardization are barriers to large-scale critical illness EHR studies.

A consortium of critical care physicians and data scientists from eight US healthcare systems developed the Common Longitudinal Intensive Care Unit (ICU) data Format (CLIF), an open-source database format that harmonizes a minimum set of ICU Data Elements for use in critical illness research. We created a pipeline to process adult ICU EHR data at each site. 

## Why having the CLIF-MIMIC dataset is useful (@Kaveri)
- study replication/validation/reproducibility


# Methods
> *The "Methods" and "Technical Implementation" sections provide details of the procedures used to create your resource including, but not limited to, how the data was collected, any measurement devices, etc. For software, the section may cover aspects such as development process, software design, and description of algorithms. For data, the section may include details such as experimental design, data acquisition, and data processing.*

The dataset is created following a three-step process: (1) search, (2) map, and (3) code. 

(1) Search: Extensive search of the original MIMIC-IV database is conducted to identify candidate data elements based on key string matching as well as consulting the extensive online documentation on both the MIMIC website and created by the github community. A suite of utilty functions are created to automatically generate a listing of all the candidate MIMIC data elements based on keyword search along with relevant statistics such as min, mean, median, and max (for numeric data elements) or discrete categories and their count of appearance (for categorical data elements) to aid the decision making in the mapping process. < add an example? > 

(2) Map: These candidate MIMIC data elements, along with their relevant statistics, are then comprehensively documented in an open-access spreadsheet and mapped to their CLIF counterparts by one to three expert physician scientists and one to two data scientists. In most cases, common decision labels are used to mark the mapping decision of a candidate MIMIC data element, e.g. whether it should be directly mapped as is, further processed such as having it unit of measurement converted, or disregarded. Examples of these common decision labels include "TO MAP, AS IS", "NO MAPPING", "TO MAP, CONVERT UOM", "UNSURE", "NOT AVAILABLE." Likewise, similar common labels are created to mark the mapping status of each candidate data element, such as its last reviewer (or the lack thereof) and if additional actions are required.



# Data Description
> *Content description: Your content (data, software, model) description should describe the resource in detail, outlining how files are structured, file formats, and a description of what the files contain. We also suggest including summary statistics where appropriate (e.g. total number of distinct patients, number of files, types of signals, over what time span was the data collected, etc.).*




# Usage Notes
> *This section should provide the reader with information relevant to reuse. Why is this data useful for the community?*
> - *In particular we suggest discussing: (1) how the data has already been used (citing relevant papers); (2) the reuse potential of the dataset; (3) known limitations that users should be aware of when using the resource; and (4) any complementary code or datasets that might be of interest to the user community.*

- cite future published papers
- highlight as a validation / reproducibility dataset

- known limitations


## the reuse potential of the dataset (@Kaveri)



# Release Notes
>*Important notes about the current release, and changes from previous versions.*



# Acknowledgements
> *Thank the people who helped with the research but did not qualify for authorship. In addition, provide any funding information.*


# Conflicts Of Interest
> *A statement on potential conflicts of interest is required. If the authors have no conflicts of interest, the section should say "The author(s) have no conflicts of interest to declare".*


# Ethics
> *Please provide a statement on the ethics of your work. Think about the project impact and briefly highlight both benefits and risks. Please also add relevant institutional review details here, for example:*
> - *Clinical trial data: Please specify trial registration number and registry name.*


# Example MIMIC-based Submissions
- [Symile-MIMIC: a multimodal clinical dataset of chest X-rays, electrocardiograms, and blood labs from MIMIC-IV](https://physionet.org/content/symile-mimic/1.0.0/)
- [MIMIC-IV-Ext-CEKG: A Process-Oriented Dataset Derived from MIMIC-IV for Enhanced Clinical Insights](https://physionet.org/content/mimic-iv-ext-cekg/1.0.0/)
- [MIMIC-IV on FHIR](https://physionet.org/content/mimic-iv-fhir/2.1/)
