
import sys
import os
sys.path.append(os.path.abspath("stages"))
import fetch_data

api_url = "https://cebs-ext.niehs.nih.gov/datasets/api/dataset/data/fetch"
_token = "Yk9ru1CcmxkmgOhbtYh2yXxQ3ZA0W8b67Gsv2wRT"  # Payload parameters - CSRF token (cross-site request forgery)
url = "https://cebs-ext.niehs.nih.gov/datasets/"

df = fetch_data.get_html_table(url, get_name_links=True)
df['slug'] = df['Link'].apply(fetch_data.get_slug)

# -------------------------------------------------------
# DEFINE SLUG
slug = 'ntp-pathology'
link = df.loc[df['slug']==slug, 'Link'].values[0]
columnList = fetch_data.get_columnList(link)

df_data, status = fetch_data.get_html_table_api(
    api_url, 
    slug, 
    columnList=columnList,
    _token=_token
)
print(slug, len(df_data))

# Notes
# Re-run successful on all of 'Failed to retrieve':
# Re-run still incomplete on 'ames' (but stops at 59,000 instead of 51,000)
# Re-run still incomplete on 'inlife-bodyweight-iad-2024' (but stops at 167,000 instead of 7.3million)


# Failed to retrieve: 
# trf
# 2015-acgih-tlvs
# neonicotinoid-health-effects-by-evidence-stream
# hallmark-geneset-annotation
# tgx-ddi-biomarker-pos
# locator-ddd
# modifier-ddd
# morphology-ddd
# biomarker-data-collection

# Incomplete: 
# ames (51,000 / 64,225)
# inlife-bodyweight-iad-2024 (7,297,000 / 7,580,510)
# ntp-pathology (22,000 / 536,930)
