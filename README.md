# Travel Blog Data Analysis

This repository contains data analysis and insights for an imaginary travel blog owned by a world traveler and solo entrepreneur. The analysis is based on a dataset reflecting user behavior, marketing efforts, and product sales on the blog.

## Overview
The dataset is a log file with ~600.000 rows. It includes information on various events such as reads, subscriptions, and purchases, segmented by country, source, and topic. The primary goal is to derive actionable insights to optimize marketing strategies, content creation, and revenue generation for the owner.

## Contents
- Data Analysis:
  - I created separate CSV files for the events extracted from the original log file using Bash because they had a different structure.
  - Next, I cleaned the data using Python scripts within a Jupyter Notebook.
  - Following the data cleaning process, I used SQL to query the dataset and extract valuable insights. I found that in this particular case SQL is better suited for segmentation purposes.
- Presentation: A slide deck summarizing key findings, strategies, and recommendations. I generated visualizations using Looker Studio.
- README: Information about the project, dataset, analysis, and instructions for replicating the analysis.

## Key Insights
- Breakdown of user behavior by event type, country, source, and topic.
- ROI analysis for marketing sources (Reddit, SEO, AdWords) and recommended budget reallocation.
- Recommendations for content creation based on user interests in different regions.
- Identification of high-potential countries for targeting based on revenue and conversion rates.
- Investigation into sudden drops in purchases and subscriptions.
- Strategies for improving subscription methods and retaining potential buyers in the sales funnel.
