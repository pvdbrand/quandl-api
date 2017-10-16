{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, TupleSections, FlexibleInstances #-}

-- |
-- Module: Data.Quandl
-- Copyright: (c) 2014 Peter van den Brand
-- License: BSD3
-- Maintainer: Peter van den Brand <peter@vdbrand.nl>
-- Stability: experimental
-- Portability: portable
--
-- This library provides an easy way to download data from Quandl.com.
-- See <http://www.quandl.com/help/api> for detailed information on the Quandl API.
--
-- For basic usage, see the 'getTable' function. This function is all you need to download tables.
--
-- For more advanced usage, see the 'getTableWith' function. This function allows you
-- to use query a subset of the data, query multiple tables (multisets),
-- apply frequency conversions, and apply transformations supported by Quandl.

module Data.Quandl (

        -- * Download a whole table at once
        getTable,

        -- * Download using API parameters
        defaultOptions,
        getTableWith,

        -- * Search for a table
        search,

        -- * Datatypes
        Options(..),
        Frequency(..),
        Transformation(..),
        Dataset(..),
        Metadata(..),
        SearchSource(..),
        SearchDoc(..),
        SearchPage(..),

        -- * Low-level functions
        downloadJSON,
        createUrl
    ) where

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Char8 as BC
import qualified Data.Text as T
import qualified Data.HashMap.Strict as H

import Data.Char (toLower, toUpper)
import Data.Time (UTCTime, Day, readTime)
import Data.Monoid ((<>))
import Data.Generics (Data, Typeable)
import Data.List (intercalate)
import Data.Aeson (decode', Value(..), (.:), (.:?), FromJSON(..))
import Data.Aeson.Types (Parser)
import Control.Monad (mzero)
import Control.Applicative ((<$>), (<*>), pure)
import Data.Time.Locale.Compat (defaultTimeLocale)

import Network.HTTP.Conduit (simpleHttp)
import Network.HTTP.Types.URI (encodePathSegments, renderQueryBuilder)
import Blaze.ByteString.Builder (toByteString, fromByteString)

-- | API parameters supported by Quandl, for use with the 'getTableWith' function.
--   See <http://www.quandl.com/help/api> for detailed information.
data Options = Options {
        opAuthToken         :: Maybe String,            -- ^ The Quandl Auth Token ('Nothing' means no token supplied, limited to 50 requests per day)
        opSortAscending     :: Bool,                    -- ^ Sort results ascending or descending
        opNumRows           :: Maybe Int,               -- ^ Limits the number of returned rows ('Nothing' means no limit)
        opStartDate         :: Maybe Day,               -- ^ Start date of returned results (inclusive, 'Nothing' means no restriction on start date)
        opEndDate           :: Maybe Day,               -- ^ End date of returned results (inclusive, 'Nothing' means no restriction on end date)
        opFrequency         :: Maybe Frequency,         -- ^ Desired frequency of returned results ('Nothing' means frequency of original dataset)
        opTransformation    :: Maybe Transformation,    -- ^ Desired transformation of returned results ('Nothing' means no transformation)
        opMetadataOnly      :: Bool                     -- ^ Only return metadata, do not return any data (only works for single table queries)
    } deriving (Eq, Ord, Show, Data, Typeable)

-- | Desired frequency of returned results.
--   See <http://www.quandl.com/help/api> for detailed information.
data Frequency
    = Daily
    | Weekly 
    | Monthly 
    | Quarterly 
    | Annual 
    deriving (Eq, Ord, Show, Data, Typeable)

-- | Desired transformation of returned results.
--   See <http://www.quandl.com/help/api> for detailed information.
data Transformation
    = Diff 
    | RDiff 
    | Cumul 
    | Normalize 
    deriving (Eq, Ord, Show, Data, Typeable)

-- | Metadata of a table. 
--   Only returned by Quandl when downloading from a single table.
data Metadata = Metadata {
        meId            :: Int,         -- ^ Table ID
        meSourceCode    :: String,      -- ^ Source code (can be used as parameter to 'getTable' and 'getTableWith')
        meTableCode     :: String,      -- ^ Table code (can be used as parameter to 'getTable' and 'getTableWith')
        meSourceName    :: T.Text,      -- ^ Human-readable name of the source
        meTableName     :: T.Text,      -- ^ Human-readable name of the table
        meUrlName       :: T.Text,      -- ^ Urlized name of the table as used on Quandl.com
        meDescription   :: T.Text,      -- ^ Description of the table
        meSourceUrl     :: T.Text,      -- ^ URL of the original data source
        meUpdatedAt     :: UTCTime      -- ^ Timestamp of latest update
    } deriving (Eq, Ord, Show, Data, Typeable)

-- | Results from a Quandl API call.
data Dataset = Dataset {
        daTable         :: Maybe Metadata,      -- ^ Metadata of the table ('Nothing' if fields from multiple tables are downloaded)
        daColumnNames   :: [T.Text],            -- ^ The column names of the table
        daData          :: [[T.Text]],          -- ^ The contents of the table
        daFromDate      :: Day,                 -- ^ The starting date of the returned data (inclusive)
        daToDate        :: Day,                 -- ^ The ending date of the returned data (inclusive)
        daFrequency     :: T.Text               -- ^ The frequency of the returned data (daily, monthly, etc)
    } deriving (Eq, Show, Data, Typeable)

-- | Results from Search calls.
data SearchSource = SearchSource {
        ssName          :: T.Text,              -- ^ Name of the source
        ssId            :: Int,                 -- ^ Source ID
        ssDescription   :: T.Text,              -- ^ Description of source
        ssHost          :: T.Text,              -- ^ Source Host site
        ssDatasetsCount :: Int,                 -- ^ count of datasets on host
        ssCode          :: T.Text               -- ^ Code to use in API.
    } deriving (Eq, Show, Data, Typeable)

data SearchDoc = SearchDoc {
        sdSourceCode       :: T.Text,           -- ^ Source code to use in API.
        sdDisplayUrl       :: Maybe T.Text,     -- ^ URL to fetch original data from.
        sdPrivate          :: Bool,             -- ^ Whether or not data is private
        sdUrlizeName       :: T.Text,           -- ^ Url encoded doc name.
        sdName             :: T.Text,           -- ^ Doc name.
        sdFromDate         :: Day,              -- ^ First date in source.
        sdDescription      :: T.Text,           -- ^ Description of data.
        sdColumnNames      :: [T.Text],         -- ^ Names of columns in data.
        sdFrequency        :: T.Text,           -- ^ Frequency of updates.
        sdSourceName       :: T.Text,           -- ^ Name of source database.
        sdUpdatedAt        :: UTCTime,          -- ^ Time of most recent updated.
        sdToDate           :: Day,              -- ^ Last day data is available.
        sdCode             :: T.Text            -- ^ Doc code to use in API.
    } deriving (Show, Eq, Data, Typeable)

data SearchPage = SearchPage {
        spTotalCount  :: Int,                   -- ^ Number of results available
        spCurrentPage :: Int,                   -- ^ Current page of results
        spPerPage     :: Int,                   -- ^ Results per page
        spSources     :: [SearchSource],        -- ^ Metadata for sources.
        spDocs        :: [SearchDoc]            -- ^ Actual documents found.
    } deriving (Eq, Show, Data, Typeable)

-------------------------------------------------------------------------------
-- JSON Parsers

asRows :: Maybe [[Value]] -> [[T.Text]]
asRows Nothing  = [[]]
asRows (Just x) = map (map convert) x
    where
        convert (Object o)  = T.pack $ show o
        convert (Array a)   = T.pack $ show a
        convert (String s)  = s
        convert (Number n)  = T.pack $ show n
        convert (Bool b)    = T.pack $ show b
        convert (Null)      = T.empty

asUTCTime :: String -> UTCTime
asUTCTime = readTime defaultTimeLocale "%FT%T%QZ"

asDay :: String -> Day
asDay = readTime defaultTimeLocale "%F"

parseMetadata :: Value -> Parser (Maybe Metadata)
parseMetadata o@(Object obj) = case H.lookup "id" obj of
                        Nothing -> pure Nothing
                        Just _  -> parseJSON o
parseMetadata _ = pure Nothing

instance FromJSON Metadata where
    parseJSON (Object v) = Metadata <$>
        v .: "id" <*>
        v .: "source_code" <*>
        v .: "code" <*>
        v .: "source_name" <*>
        v .: "name" <*>
        v .: "urlize_name" <*>
        v .: "description" <*>
        v .: "display_url" <*>
        (asUTCTime <$> v .: "updated_at")
    parseJSON _ = mzero

instance FromJSON Dataset where
    parseJSON o@(Object v) = Dataset <$>
        parseMetadata o <*>
        v .: "column_names" <*>
        (asRows <$> v .:? "data") <*>
        (asDay  <$> v .: "from_date") <*>
        (asDay  <$> v .: "to_date") <*>
        v .: "frequency"
    parseJSON _ = mzero

instance FromJSON SearchSource where
    parseJSON (Object v) = SearchSource <$>
        v .: "name" <*>
        v .: "id" <*>
        v .: "description" <*>
        v .: "host" <*>
        v .: "datasets_count" <*>
        v .: "code"
    parseJSON _ = mzero

instance FromJSON SearchDoc where
    parseJSON (Object v) = SearchDoc <$>
        v .: "source_code" <*>
        v .: "display_url" <*>
        v .: "private" <*>
        v .: "urlize_name" <*>
        v .: "name" <*>
        (asDay <$> v .:"from_date") <*>
        v .: "description" <*>
        v .: "column_names" <*>
        v .: "frequency" <*>
        v .: "source_name" <*>
        (asUTCTime <$> v .: "updated_at" ) <*>
        (asDay <$> v .: "to_date") <*>
        v .: "code"
    parseJSON _ = mzero

instance FromJSON SearchPage where
    parseJSON (Object v) = SearchPage <$>
        v .: "total_count" <*>
        v .: "current_page" <*>
        v .: "per_page" <*>
        v .: "sources" <*>
        v .: "docs"
    parseJSON _ = mzero

-------------------------------------------------------------------------------
-- Public functions

-- | Default options to use for Quandl API calls.
--   The default options do not use an auth token
--   and will return all rows in descending order.
defaultOptions :: Options
defaultOptions = Options {
    opAuthToken         = Nothing,
    opSortAscending     = False,
    opNumRows           = Nothing,
    opStartDate         = Nothing,
    opEndDate           = Nothing,
    opFrequency         = Nothing,
    opTransformation    = Nothing,
    opMetadataOnly      = False
}

-- | Download all rows and columns from a single table.
--   To get all data points for the dataset FRED/GDP:
--
--   > getTable "FRED" "GDP" Nothing
--
--   Registered users should include their auth_token, like this:
--
--   > getTable "FRED" "GDP" (Just "dsahFHUiewjjd")
getTable :: String              -- ^ Quandl code for the source
         -> String              -- ^ Quandl code for the table
         -> Maybe String        -- ^ Auth code
         -> IO (Maybe Dataset)  -- ^ Dataset returned by Quandl, or 'Nothing' if parsing failed
getTable source table auth = decode' <$> downloadJSON (defaultOptions { opAuthToken = auth }) [(source, table, Nothing)]

-- | Download data from Quandl using the full API.
--   This function supports all data manipulation options, plus downloading from multiple datasets (multisets).
--
--   For example, here is the annual percentage return for AAPL stock over the previous decade, in ascending date order:
--
--   > import Data.Quandl
--   > import Data.Time (fromGregorian)
--   > getTableWith (defaultOptions {opSortAscending  = True, 
--   >                               opStartDate      = Just (fromGregorian 2000 1 1), 
--   >                               opEndDate        = Just (fromGregorian 2010 1 1), 
--   >                               opFrequency      = Just Annual, 
--   >                               opTransformation = Just RDiff})
--   >              [("WIKI", "AAPL", Just 4)]  -- Just 4 means we only want the 4'th column (Close price)
--
--   You can pull data from multiple datasets (or from multiple columns in a single dataset) using this function as well.
--   In the example below, we combine US GDP from FRED\/GDP, crude oil spot prices from DOE\/RWTC, and Apple closing prices from WIKI\/AAPL. 
--   We are going to convert all of them to annual percentage changes, and look only at data for the last 10 years.
--
--   > import Data.Quandl
--   > getTableWith (defaultOptions {opNumRows        = Just 10,
--   >                               opFrequency      = Just Annual,
--   >                               opTransformation = Just RDiff})
--   >              [("FRED", "GDP", Just 1), ("DOE", "RWTC", Just 1), ("WIKI", "AAPL", Just 4)]
--
--   Please note that Quandl does not return the table metadata when pulling data from multiple datasets.
--   In that case the 'daTable' field of the returned 'Dataset' will be 'Nothing'.
getTableWith :: Options                         -- ^ API parameters
             -> [(String, String, Maybe Int)]   -- ^ List of (source, table, column) items to retrieve. 'Nothing' retrieves all columns.
             -> IO (Maybe Dataset)              -- ^ Dataset returned by Quandl, or 'Nothing' if parsing failed
getTableWith options items = decode' <$> downloadJSON options items

-- | Returns unparsed JSON from a Quandl API call.
downloadJSON :: Options -> [(String, String, Maybe Int)] -> IO L.ByteString
downloadJSON options items = simpleHttp $ createUrl options items

-- | Construct a URL given the various API parameter options.
createUrl :: Options -> [(String, String, Maybe Int)] -> String
createUrl options items =
    let pathSegs = case items of
                        [(source, table, _)] -> ["api", "v1", "datasets", T.pack (map toUpper source), T.pack (map toUpper table ++ ".json")]
                        _                    -> ["api", "v1", "multisets.json"]
        column = case items of
                        [(_, _, c)] -> maybeParam ("column",  fmap show . const c)
                        _           -> param      ("columns", createColumns items)
        query = column ++
                concatMap maybeParam [
                    ("auth_token",     opAuthToken),
                    ("rows",           fmap show . opNumRows),
                    ("trim_start",     fmap show . opStartDate),
                    ("trim_end",       fmap show . opEndDate),
                    ("collapse",       fmap (map toLower . show) . opFrequency),
                    ("transformation", fmap (map toLower . show) . opTransformation)] ++
                concatMap param [
                    ("sort_order",      if opSortAscending options then "asc" else "desc"),
                    ("exclude_data",    if opMetadataOnly options then "true" else "false")
                ]
        maybeParam (k, fn) = maybe [] (param . (k,)) $ fn options
        param      (k, v)  = [(k, Just $ BC.pack v)]
    in BC.unpack $ toByteString $
        fromByteString "http://www.quandl.com" <>
        encodePathSegments pathSegs <>
        renderQueryBuilder True query

createColumns :: [(String, String, Maybe Int)] -> String
createColumns items =
    let toItem (source, table, column) = map toUpper source ++ "." ++ map toUpper table ++ (case column of { Nothing -> ""; Just c -> "." ++ show c })
    in  intercalate "," $ map toItem items

-- | Search for terms, returning given page
search :: [String]                     -- ^ List of search terms
          -> Maybe String              -- ^ Auth Token
          -> Maybe Int                 -- ^ Number to display per page or Nothing
          -> Maybe Int                 -- ^ Page number to display or Nothing
          -> IO (Maybe SearchPage)     -- ^ Search results from Quandl, or Nothing if parsing failed.
search terms token per_page page = decode' <$> simpleHttp makeUrl where
  query = concatMap param [Just ("query", intercalate "+" terms),
                           fmap (\t -> ("auth_token", t)) token,
                           fmap (\p -> ("page", show p))  page,
                           fmap (\p -> ("per_page", show p)) per_page]
  param (Just (k, v)) = [(BC.pack k, Just $ BC.pack v)]
  param Nothing       = []
  makeUrl = BC.unpack $ toByteString $
            fromByteString "http://www.quandl.com/api/v1/datasets.json" <>
            renderQueryBuilder True query
