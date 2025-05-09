import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Load the dataset
df = pd.read_csv('adult_dataset.csv')

# Histogram: Distribution of Age
df['age'].plot.hist(title='Age Distribution')
plt.xlabel('Age')
plt.show()

# Dot Plot: Education Number
plt.figure(figsize=(10, 6))
education_counts = df['education'].value_counts().sort_index()
plt.plot(education_counts.index, education_counts.values, 'o')
plt.xticks(rotation=45)
plt.xlabel('Education Level')
plt.ylabel('Count')
plt.title('Count of each Education Levels')s
plt.show()


# Bar Plot: Workclass Counts
df['workclass'].value_counts().plot.bar(title='Workclass Counts')
plt.ylabel('Count')
plt.show()

# Line Chart: Average Hours per Week by Age
df.groupby('age')['hours-per-week'].mean().plot(title='Avg Hours per Week by Age')
plt.ylabel('Hours per Week')
plt.show()

# Box Plot with Histogram and Scatter Overlay
# Improved: Box Plot with Strip Overlay
plt.figure(figsize=(10, 6))
sns.boxplot(x='income', y='hours-per-week', data=df)
sns.stripplot(x='income', y='hours-per-week', data=df, color='orange', alpha=0.3, jitter=0.2)
plt.title('Hours per Week by Income')
plt.show()

