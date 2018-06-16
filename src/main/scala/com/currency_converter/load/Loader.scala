package com.currency_converter.load

import com.currency_converter.model.ExchangeRate

import scala.io.Source

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** Functions called when initializing CurrencyConverter in order to load data.
  *
  * @author Xavier Guihot
  * @since 2017-02
  */
private[currency_converter] object Loader extends Serializable {

  /** Loads data (currency exchange rates).
    *
    * Here is a sample of what data looks like once rates are loaded:
    *
    * Map(
    *   "20170118": Map(
    *     "SEK" -> 0.120043d,
    *     "CZK" -> 0.041248d,
    *     "EUR" -> 1.1145d
    *   ),
    *   "20170119": Map(
    *     "SEK" -> 0.12112d,
    *     "CZK" -> 0.04175d,
    *     "EUR" -> 1.10945d
    *   )
    * )
    *
    * We only load rates from a currency to USD in order to avoid loading too
    * much rates into memory (when considering loading a year of rates, if the
    * cartesian product of all currencies is loaded for each day, this might
    * start using too much memory).
    *
    * @param currencyFolder the path to the folder which contains currencies
    * @param sparkContext if None, then files are loaded from a classical file
    * system; if Some(SparkContext), then files are loaded with Spark from HDFS.
    * @param firstDateOfRates the first date of exchange rates to use
    * @param lastDateOfRates the last date of exchange rates to use. If it is
    * "" then the last date is considered to be yesterday.
    * @param parseRateLine the functions which parses a raw exchange rate
    * @return the exchange rate map
    */
  def loadExchangeRates(
      currencyFolder: String,
      sparkContext: Option[SparkContext],
      firstDateOfRates: String,
      lastDateOfRates: String,
      parseRateLine: String => Option[ExchangeRate]
  ): Map[String, Map[String, Double]] = {

    val toUsdRates = sparkContext match {

      case Some(sc) =>
        loadExchangeRatesFromHdfs(
          sc.textFile(currencyFolder),
          firstDateOfRates,
          lastDateOfRates,
          parseRateLine)

      case None =>
        loadExchangeRatesFromFs(
          currencyFolder,
          firstDateOfRates,
          lastDateOfRates,
          parseRateLine)
    }

    // Obviously, we can't go on if nothing was loaded:
    require(toUsdRates.nonEmpty, "no exchange rates found.")

    toUsdRates
  }

  /** Loads the rate map from an RDD of rates.
    *
    * @param rawRates the RDD of raw rates (such as "2017-03-27,USD,,SEK,,,8.811")
    * @param firstDateOfRates the first date of exchange rates to use
    * @param lastDateOfRates the last date of exchange rates to use
    * @param parseRateLine the functions which parses a raw exchange rate
    * @return the exchange rate map
    */
  def loadExchangeRatesFromHdfs(
      rawRates: RDD[String],
      firstDateOfRates: String,
      lastDateOfRates: String,
      parseRateLine: String => Option[ExchangeRate]
  ): Map[String, Map[String, Double]] = {

    println("CurrencyConverter: Loading exchange rates from hdfs")

    rawRates
      .flatMap(parseRateLine(_))
      .filter {
        case ExchangeRate(date, fromCurr, toCurr, _) =>
          date >= firstDateOfRates && date <= lastDateOfRates &&
            (fromCurr == "USD" || toCurr == "USD")
      }
      .groupBy(_.date)
      .mapValues(
        usdRates =>
          usdRates.map {
            case ExchangeRate(_, fromCurr, "USD", rate) => (fromCurr, rate)
            case ExchangeRate(_, "USD", toCurr, rate)   => (toCurr, 1d / rate)
          }.toMap
      )
      .collect()
      .toMap
  }

  /** Loads the rate map from a folder of exchange rate files on a classic file
    * system.
    *
    * @param currencyFolder the path to the folder which contains currencies
    * @param firstDateOfRates the first date of exchange rates to use
    * @param lastDateOfRates the last date of exchange rates to use
    * @param parseRateLine the functions which parses a raw exchange rate
    * @return the exchange rate map
    */
  def loadExchangeRatesFromFs(
      currencyFolder: String,
      firstDateOfRates: String,
      lastDateOfRates: String,
      parseRateLine: String => Option[ExchangeRate]
  ): Map[String, Map[String, Double]] = {

    println("CurrencyConverter: Loading exchange rates")

    val folder = new File(currencyFolder)

    require(folder.exists, s"folder $currencyFolder doesn't exist")
    require(
      folder.isDirectory,
      s"folder $currencyFolder is a file; expecting a folder"
    )

    val currencyFiles = folder.listFiles.filter(_.isFile).toList

    currencyFiles
      .flatMap(
        currencyFile =>
          // Let's parse from each file of rate (usually one file per date)
          // the different rates:
          Source
            .fromFile(currencyFile, "UTF-8")
            .getLines
            .flatMap(parseRateLine(_))
            .filter {
              case ExchangeRate(date, fromCurr, toCurr, _) =>
                date >= firstDateOfRates && date <= lastDateOfRates &&
                  (fromCurr == "USD" || toCurr == "USD")
          })
      .groupBy(_.date)
      .mapValues(
        usdRates =>
          usdRates.map {
            case ExchangeRate(_, fromCurr, "USD", rate) => (fromCurr, rate)
            case ExchangeRate(_, "USD", toCurr, rate)   => (toCurr, 1d / rate)
          }.toMap
      )
  }
}
