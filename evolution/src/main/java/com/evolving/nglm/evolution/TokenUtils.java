/*****************************************************************************
 *
 *  TokenUtils.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class TokenUtils
{

  private static Random random = new Random();

  /*****************************************
   *
   *    Finite-state machine State
   *
   *****************************************/

  private enum State
  {
    REGULAR_CHAR,
    REGULAR_CHAR_AFTER_BRACKET,
    LEFT_BRACKET,
    LEFT_BRACE,
    ERROR;
  }

  /*****************************************
   *
   *  generateFromRegex
   *
   *****************************************/
  /**
   * Generate a string that matches the regex passed in parameter.
   * Code supports simple patterns, such as :
   *   [list of chars]{numbers}      ( "{numbers}" can be omitted and defaults to 1)
   * There can be as many of these patterns as needed, mixed with regular chars that are not interpreted.
   * Example of valid regex :
   *   gl-[0123456789abcdef]{5}-[01234]
   *   [ACDEFGHJKMNPQRTWXY34679]{5}
   *   tc-[ 123456789][0123456789]{7}-tc
   * @param regex regex to generate from.
   * @return generated string, or an error message if regex has an invalid syntax.
   */
  public static String generateFromRegex(String regex)
  {
    StringBuilder output = new StringBuilder();
    StringBuilder chooseFrom = new StringBuilder();
    StringBuilder numOccurences = new StringBuilder();
    State currentState = State.REGULAR_CHAR;

    for (char c : regex.toCharArray())
      {
        switch (currentState)
        {

          case REGULAR_CHAR:
            if (c == '[')
              {
                currentState = State.LEFT_BRACKET;
              }
            else if (c == '{')
              {
                currentState = State.LEFT_BRACE;
              } 
            else
              {
                output.append(String.valueOf(c)); // Regular char goes to output string
              }
            break;

          case REGULAR_CHAR_AFTER_BRACKET:
            if (c == '{')
              {
                currentState = State.LEFT_BRACE;
              } 
            else
              {
                generateRandomString(chooseFrom, 1, output); // No braces after [...] means 1 occurrence
                chooseFrom = new StringBuilder();
                if (c == '[')
                  {
                    currentState = State.LEFT_BRACKET;
                  } 
                else
                  {
                    output.append(String.valueOf(c));
                    currentState = State.REGULAR_CHAR;
                  }
              }
            break;

          case LEFT_BRACKET:
            if (c == ']')
              {
                currentState = State.REGULAR_CHAR_AFTER_BRACKET;
              } 
            else if (c == '{')
              {
                output.append("-INVALID cannot have '{' inside brackets");
                currentState = State.ERROR;
              } 
            else
              {
                chooseFrom.append(String.valueOf(c));
              }
            break;

          case LEFT_BRACE:
            if (c == '}')
              {
                if (numOccurences.length() == 0)
                  { 
                    output.append("-INVALID cannot have '{}'");
                    currentState = State.ERROR;
                  } 
                else if (chooseFrom.length() == 0)
                  {
                    output.append("-INVALID cannot have []{n}");
                    currentState = State.ERROR;
                  }
                else
                  {
                    int numberOfOccurrences = Integer.parseInt(numOccurences.toString());
                    generateRandomString(chooseFrom, numberOfOccurrences, output);
                    chooseFrom = new StringBuilder();
                    numOccurences = new StringBuilder();
                    currentState = State.REGULAR_CHAR;
                  }
              } 
            else if (c == '[')
              {
                output.append("-INVALID cannot have '[' inside braces");
                currentState = State.ERROR;
              } 
            else
              {
                numOccurences.append(String.valueOf(c));
              }
            break;

          case ERROR:
            return output.toString();
        }
      }

    //
    // Check final state (after processing all input)
    //
    switch (currentState)
    {
      case REGULAR_CHAR_AFTER_BRACKET:
        generateRandomString(chooseFrom, 1, output); // No braces after [...] means 1 occurrence
        break;
      case LEFT_BRACKET:
      case LEFT_BRACE:
        output.append("-INVALID cannot end with pending '{' or '['");
        break;
      case REGULAR_CHAR:
      case ERROR:
        break;
    }

    return output.toString();
  }
  /*****************************************
   *
   *  generateRandomString
   *
   *****************************************/

  private static void generateRandomString(StringBuilder possibleValues, int length, StringBuilder result)
  {
    // System.out.println("possibleValues = "+possibleValues+" length = "+length);
    for (int i=0; i<length; i++)
      {
        result.append(chooseRandomOne(possibleValues.toString()));
      }
  }

  /*****************************************
   *
   *  chooseRandomOne
   *
   *****************************************/

  private static String chooseRandomOne(String values)
  {
    int index = random.nextInt(values.length());
    String result = values.substring(index, index+1);
    return result;
  }

  /*****************************************
   *
   *  isValidRegex
   *
   *****************************************/

  /**
   * Checks is the given regex is valid or not.
   * @param regex
   * @return true iif the regex has a valid syntax.
   */
  public static boolean isValidRegex(String regex)
  {
    boolean res = true;
    try
    {
      Pattern.compile(regex);
    } 
    catch (PatternSyntaxException ex)
    {
      res = false;
    }
    return res;
  }

  /*****************************************
   *
   *  main
   *
   *****************************************/

  public static void main(String[] args)
  {
    String[] testStrings = new String[]
        {
            // Token formats from E4O
            "br-[0123456789abcdef]{5}",
            "sl-[0123456789abcdef]{5}",
            "gl-[0123456789abcdef]{5}",
            "[ACDEFGHJKMNPQRTWXY34679]{5}",
            "[ACDEFGHJKMNPQRTWXY34679]{6}",
            "[ACDEFGHJKMNPQRTWXY34679]{7}",

            // Test values
            "gl-[0123456789abcdef]{5}-[01234]",
            "0x[ABCDEF0123456789]{4}-0x[ABCDEF0123456789]{2}--0x[ABCDEF0123456789]{8}-0x[ABCDEF0123456789]{2}",
            "tc-[ 123456789][0123456789]{7}-tc",
            "A-[1]{3}-B",
            "A-[1",
            "A-{",
            "A-{[[[[[",
            "A-[123]{a[",
            "I am [ 1][123456789][0123456789] years old"
        };
    for (String regex : testStrings)
      {
        if (isValidRegex(regex))
          {
            try
            {
              String result = TokenUtils.generateFromRegex(regex);
              System.out.println("Generated \""+result+"\" from regex \""+regex+"\"");
              //
              // Check that the generated string matches the regex
              //
              Pattern pattern = Pattern.compile("^"+regex+"$");
              Matcher matcher = pattern.matcher(result);
              if (!matcher.find())
                {
                  System.out.println("PROBLEM : should match");
                }
            }
            catch (PatternSyntaxException ex)
            {
              System.out.println("PROBLEM : unexpected issue with regex "+regex);
            }
          }
        else
          {
            System.out.println("We should get an issue generating from regex "+regex);
            String result = TokenUtils.generateFromRegex(regex);
            System.out.println("    ----> Generated \""+result+"\" from regex \""+regex+"\"");
          }
      }
    System.out.println("All tests finished");
  }
}
