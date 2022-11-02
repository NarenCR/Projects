import pandas as pd

df = pd.DataFrame(columns=['ID', 'Name', 'Last name', 'Position', 'Age'])
file =open('D:\FSDS\Resouces\Pandas\data_Extraction_1.txt','r')
L = file.readlines()
line_count = 0
for line in L:
    d={}
    s = line[1:len(line)-2]
    k =s.split('\\n')
    for i in k:
       t = i.split(': ')
       d[t[0]] = t[1]
    line_count += 1
    df = df.append(d,ignore_index=True)
print(df)


