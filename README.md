## Google Analytics Customer Revenue Prediction System


 - This repository represents "Google Analytics Customer Revenue Prediction System "
 - With the help of this project we can Predict the potential future business.


##  📝 Description


Create an automated system for predicting potential future business, finding potential
customers based on the various parameters as decided by the machine learning
algorithm.using the GStore data we need to predict the future revenue will be
created by those customers.

## ⏳ Dataset

- Download the dataset for custom training
- https://www.kaggle.com/c/ga-customer-revenue-prediction/data


### :hammer_and_wrench: Requirements

- python > 3.6 
- pandas
- numpy
- mongodb
- XGBoost
- LGB
- sklearn

### 🎯 Model
- LightGBM  model got selected for predictiing the revenues
- LightGBM is a gradient boosting framework based on decision trees to increases the efficiency of the model and reduces memory usage
- Model Architecture :
     LightGBM splits the tree leaf-wise as opposed to other boosting algorithms that grow tree level-wise. It chooses the leaf with maximum delta loss to grow. Since the leaf is    fixed, the leaf-wise algorithm has lower loss compared to the level-wise algorithm. Leaf-wise tree growth might increase the complexity of the model and may lead to              overfitting in small datasets.
   ![Leaf-Wise-Tree-Growth](https://user-images.githubusercontent.com/71813414/138502357-b81adcb0-c952-4c39-8645-38a96904dc24.png)

### Deployment
- Heroku cloud has been used for deployment the ML system
- https://ganalyst.herokuapp.com/   

