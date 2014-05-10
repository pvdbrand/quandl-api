# quandl-api

This library provides an easy way to download data from Quandl.com in Haskell.

## Installation

* Install the [Haskell Platform](http://www.haskell.org/platform/)
* `cabal install quandl-api`

## Basic Usage

The `getTable` function is all you need to download tables.
To get all data points for the dataset FRED/GDP:

    getTable "FRED" "GDP" Nothing

Registered users should include their auth_token, like this:

    getTable "FRED" "GDP" (Just "dsahFHUiewjjd")

## Advanced Usage

The `getTableWith` function allows you to use query a subset of the data,
query multiple tables (multisets), apply frequency conversions,
and apply transformations supported by Quandl.
For example, here is the annual percentage return for AAPL stock
over the previous decade, in ascending date order:

    import Data.Quandl
    import Data.Time (fromGregorian)
    getTableWith (defaultOptions {opSortAscending  = True, 
                                  opStartDate      = Just (fromGregorian 2000 1 1), 
                                  opEndDate        = Just (fromGregorian 2010 1 1), 
                                  opFrequency      = Just Annual, 
                                  opTransformation = Just RDiff})
                 [("WIKI", "AAPL", Just 4)]  -- Just 4 means we only want the 4'th column (Close price)

You can pull data from multiple datasets (or from multiple columns in a single dataset) using this function as well.
In the example below, we combine US GDP from FRED/GDP, crude oil spot prices from DOE/RWTC, and Apple closing prices from WIKI/AAPL. 
We are going to convert all of them to annual percentage changes, and look only at data for the last 10 years:

    import Data.Quandl
    getTableWith (defaultOptions {opNumRows        = Just 10,
                                  opFrequency      = Just Annual,
                                  opTransformation = Just RDiff})
                 [("FRED", "GDP", Just 1), ("DOE", "RWTC", Just 1), ("WIKI", "AAPL", Just 4)]

See http://www.quandl.com/help/api for detailed information on the Quandl API.

