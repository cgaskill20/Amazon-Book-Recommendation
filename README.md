# Amazon-Book-Recommendation
Amazon book recommendation system using HDFS and MapReduce

Using Amazon Book Reviews dataset: https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews?select=Books_rating.csv


### Job One
Input: Amazon Book Reviews files with first line removed.

Mapper: Outputs (userID, Book Title) for each positive review.

Reducer: Outputs (userID, List of Book Titles) for each user with more than one review.

Output: UserID TAB Title,Title,Title... (the mix of tab and commas may be difficult to read in. If so i can put a comma before the first book to split easier.

### Job Two
Input: Job One's output (Train Split) (UserID TAB Title, Title, Title...)

Mapper: Outputs (Bigrams of user's reviewed positive book titles, IntWritable(1))

Reducer: Outputs: (Bigram, number of occurences of bigram)

Output: Bigram of titles TAB # of occurences

### Job Three
Input: Job Two's output

Mapper: Outputs (Book Title, RecomendationWritable[Recomendation Title, # of Occurences]) 

Reducer: Outputs (Book Title, Comma separated sorted list of most recomended titles SPACE the titles # of Occurences) 

Output: Title TAB title 21, title 10, title 8, ...

### Recommender
Input: Job Three's output AND A portion of the origional input data used for testing

Mapper1 (Origional Input): Outputs  (Positively reviewed book title, "A " + UserID)

Mapper2 (Job Three Output): Outputs (Book Title, "B " + Recommendation List)

Reducer: Outputs (User ID, List of 3 recommendations)

Output: UserID TAB Rec1, Rec2, Rec3
