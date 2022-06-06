import json
import pycountry

# import pandas as pd

d = {
    c.alpha_3: [s.name for s in pycountry.subdivisions.get(country_code=c.alpha_2)]
    for c in pycountry.countries
}
with open("countries_subdivisions.json", "w", encoding="utf-8") as fh:
    json.dump(d, fh, ensure_ascii=False)

# pd.DataFrame([{'country': pycountry.countries.get(alpha_2=s.country_code).name, 'alpha_3': pycountry.countries.get(alpha_2=s.country_code).alpha_3, 'subdivision': s.name} for s in pycountry.subdivisions]).to_csv('countries_subdivisions.csv', index=False)
