
#16,submission9-5M,0.9418
#8,submission6full,0.9508,2018-04-10,naive bayes (full model)
#3,submission2mean,0.7720,2018-03-27,random forest
#10,submission7full,0.6718,2018-04-13,naive bayes (IP based)

import itertools
import pandas as pd

scores = {'sub2': 0.7720, 'sub6': 0.9508, 'sub7': 0.6718, 'sub9': 0.9418}

root = 'submissions/submission'

predictionDF = pd.DataFrame(pd.Series(
                   pd.read_csv(root + '2mean.csv')['click_id']))

submissions = {
    'sub2': pd.read_csv(root + '2mean.csv')['is_attributed'],
    'sub6': pd.read_csv(root + '6full.csv')['is_attributed'],
    'sub7': pd.read_csv(root + '7full.csv')['is_attributed'],
    'sub9': pd.read_csv(root + '9-5M.csv')['is_attributed']}         

#2 model combinations
combinations = list(itertools.combinations(submissions.keys(), 2))
for i in combinations:
    fName = '_'.join(i)
    print(fName)
    coefA, coefB = scores[i[0]], scores[i[1]]
    s = coefA + coefB
    coefA, coefB = coefA/s, coefB/s
    attributed = [coefA*a + coefB*b for a,b in 
       zip(submissions[i[0]], submissions[i[1]])]
    predictionDF['is_attributed'] = pd.Series(attributed)
    predictionDF.to_csv(root + '10-' + fName + '.csv', index = False)

#3 model combinations
combinations = list(itertools.combinations(submissions.keys(), 3))
for i in combinations:
    fName = '_'.join(i)
    print(fName)
    coefA, coefB, coefC = scores[i[0]], scores[i[1]], scores[i[2]]
    s = coefA + coefB + coefC
    coefA, coefB, coefC = coefA/s, coefB/s, coefC/s
    attributed = [coefA*a + coefB*b + coefC*c for a,b,c in 
       zip(submissions[i[0]], submissions[i[1]], submissions[i[2]])]
    predictionDF['is_attributed'] = pd.Series(attributed)
    predictionDF.to_csv(root + '10-' + fName + '.csv', index = False)

#4 model combination
fName = '_'.join(submissions.keys())
print(fName)
coefA, coefB, coefC, coefD = scores.values()
s = coefA + coefB + coefC + coefD
coefA, coefB, coefC, coefD = coefA/s, coefB/s, coefC/s, coefD/s
attributed = [coefA*a + coefB*b + coefC*c + coefD*d for a,b,c,d in 
    zip(submissions['sub2'], submissions['sub6'], submissions['sub7'],
        submissions['sub9'])]
predictionDF['is_attributed'] = pd.Series(attributed)
predictionDF.to_csv(root + '10-' + fName + '.csv', index = False)



