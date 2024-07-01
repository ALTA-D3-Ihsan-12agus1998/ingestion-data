import pandas as pd

arr_data = [ [10, 100, 1000], [40, 400, 4000]]
print("Dataframe created from array")
print(arr_data)
print("---------------------")

df = pd.DataFrame(arr_data)
print("The DataFrame ")
print(df)