{-# LANGUAGE OverloadedStrings, DeriveDataTypeable, TupleSections, FlexibleInstances #-}
module Data.Quandl (
        Options(..),
        Frequency(..),
        Transformation(..),
        Metadata(..),
        Dataset(..),
        defaultOptions,
        getTable,
        download,
        downloadRaw,
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
import System.Locale (defaultTimeLocale)

import Network.HTTP.Conduit (simpleHttp)
import Network.HTTP.Types.URI (encodePathSegments, renderQueryBuilder)
import Blaze.ByteString.Builder (toByteString, fromByteString)

data Options = Options {
        opAuthToken         :: Maybe String,
        opSortAscending     :: Bool,
        opExcludeHeaders    :: Bool,
        opNumRows           :: Maybe Int,
        opStartDate         :: Maybe Day,
        opEndDate           :: Maybe Day,
        opFrequency         :: Maybe Frequency,
        opTransformation    :: Maybe Transformation,
        opMetadataOnly      :: Bool
    } deriving (Eq, Ord, Show, Data, Typeable)

data Frequency
    = Daily
    | Weekly 
    | Monthly 
    | Quarterly 
    | Annual 
    deriving (Eq, Ord, Show, Data, Typeable)

data Transformation
    = Diff 
    | RDiff 
    | Cumul 
    | Normalize 
    deriving (Eq, Ord, Show, Data, Typeable)

data Metadata = Metadata {
        meId            :: Int,
        meSourceCode    :: String,
        meTableCode     :: String,
        meSourceName    :: T.Text,
        meTableName     :: T.Text,
        meUrlName       :: T.Text,
        meDescription   :: T.Text,
        meSourceUrl     :: T.Text,
        meUpdatedAt     :: UTCTime,
        mePrivate       :: Bool
    } deriving (Eq, Ord, Show, Data, Typeable)

data Dataset = Dataset {
        daTable         :: Maybe Metadata,
        daData          :: [[T.Text]],
        daFromDate      :: Day,
        daToDate        :: Day,
        daFrequency     :: T.Text, -- Frequency? Either String Frequency?
        daColumnNames   :: [T.Text]
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
        (asUTCTime <$> v .: "updated_at") <*>
        v .: "private"
    parseJSON _ = mzero

instance FromJSON Dataset where
    parseJSON o@(Object v) = Dataset <$>
        parseMetadata o <*>
        (asRows <$> v .:? "data") <*>
        (asDay  <$> v .: "from_date") <*>
        (asDay  <$> v .: "to_date") <*>
        v .: "frequency" <*>
        v .: "column_names"
    parseJSON _ = mzero

-------------------------------------------------------------------------------
-- Public functions

defaultOptions :: Options
defaultOptions = Options {
    opAuthToken         = Nothing,
    opSortAscending     = True,
    opExcludeHeaders    = False,
    opNumRows           = Nothing,
    opStartDate         = Nothing,
    opEndDate           = Nothing,
    opFrequency         = Nothing,
    opTransformation    = Nothing,
    opMetadataOnly      = False
}

getTable :: String -> String -> IO (Maybe Dataset)
getTable source table = decode' <$> downloadRaw defaultOptions [(source, table, Nothing)]

download :: Options -> [(String, String, Maybe Int)] -> IO (Maybe Dataset)
download options items = decode' <$> downloadRaw options items

downloadRaw :: Options -> [(String, String, Maybe Int)] -> IO L.ByteString
downloadRaw options items = simpleHttp $ createUrl options items

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
                    ("exclude_headers", if opExcludeHeaders options then "true" else "false"),
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

