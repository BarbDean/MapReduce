package com.barbdean.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;


public class TextInput  {

   public static ArrayList<String> Lines = new ArrayList<String>();

   // Read all of the lines out of file and return them as an arraylist.
   // The calling function will then parse the lines according to the 
   // input file format definition.
   public static ArrayList<String> readData(String[] args) { 
          // throws FileNotFoundException, IOException  {

        File dataFile = null; 
        ArrayList<String> lines = new ArrayList<String>();

        for(String arg : args){
            if(arg.startsWith("-file=")){
               dataFile = new File(arg.substring(6));
            }
        }

        // If there was not a filename specified with the -file, 
        // check to see if the first argument on the command line is a
        // valid filename
        if(dataFile==null && args.length > 0) {
           dataFile = new File(args[0]);
        }

        if(dataFile.exists()) {} 

        try {
            BufferedReader input =  new BufferedReader(new FileReader(dataFile));
            String line = null;
            while (( line = input.readLine()) != null){
                lines.add(line);
            }
            input.close();
        }
        catch (FileNotFoundException e) {
            System.err.println("FileNotFoundException: " +  e.getMessage());
            throw new Error("Exitting due to FileNotFound...");
        }
        catch (IOException e) {
             System.err.println("IOException: " +  e.getMessage());
             throw new Error("Exitting due to IOException...");
        }
        catch (Exception e) {
             System.err.println("Caught Unknown Exception: " +  e.getMessage());
             e.printStackTrace();
        }
        return lines;
    }
}
