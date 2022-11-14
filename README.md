# Amazon-Book-Recommendation
Amazon book recommendation system using HDFS and MapReduce

Using Amazon Book Reviews dataset: https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews?select=Books_rating.csv


### Job One
Input: Amazon Book Reviews files with first line removed.

Mapper: Outputs (userID, Book Title) for each positive review.

Reducer: Outputs (userID, List of Book Titles) for each user with more than one review.

Output: UserID TAB Title,Title,Title... (the mix of tab and commas may be difficult to read in. If so i can put a comma before the first book to split easier.
