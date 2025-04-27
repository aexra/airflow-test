import pandas as pd
import numpy as np

df = pd.read_csv('assets/cars.csv', sep=';')

r = df.sample(5)
r['price_usd'] = np.random.randint(10_000, 50_001, size=5)

print(r.shape[0])