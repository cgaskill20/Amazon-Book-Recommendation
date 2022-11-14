import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Scanner;

public class Review {
//       public static void main(String[] args) {
//       
//       StringBuilder stringBuilder = new StringBuilder();
// 
//       try (Scanner sc = new Scanner(new File("oneReview.txt"))) {
//            while (sc.hasNext()) {
//                 stringBuilder.append(sc.nextLine());
//                 stringBuilder.append("\n");
//             }
//       } catch (IOException e) {
// 			e.printStackTrace();
// 		}
// 		String finalString = stringBuilder.toString();
// 
//         Review review = new Review(finalString);
//       } 
//       ***********************************************END TEST MAIN****************
        
    private String bookTitle, userID;
    private boolean positiveReview;
    //Id,Title,Price,User_id,profileName,review/helpfulness,review/score,review/time,review/summary,review/text

    public Review(String reviewRow) {
	  
          String[] csvSplit = reviewRow.split(",");
          
          if(csvSplit.length < 10){
            positiveReview = false;  //discard if data point isnt valid
          }
          
          else{
          
            bookTitle = csvSplit[1];
            userID = csvSplit[3];
            
            positiveReview = posOrNegReview(csvSplit[6]);
          }
          
     }
     
    private boolean posOrNegReview(String rating) {
          
          if(rating.equals("4.0") || rating.equals("5.0")) {
            return true;
          }
          
          return false;
    } 

	  public String getBookTitle() {
		  return bookTitle;
	  }

	  public String getUserID() {
		  return userID;
	  }
     
    	  public boolean getPositiveReview() {
		  return positiveReview;
	  }
}
