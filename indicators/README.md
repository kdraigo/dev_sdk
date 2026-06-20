# dev_sdk indicators

The SDK ships ~95 TA-Lib indicators plus a few helpers. They are exposed per
timeframe through the indicator manager and computed from the candles the SDK
has streamed so far.

## Access pattern

```go
import (
    sdk "github.com/kdraigo/dev_sdk"
    "github.com/kdraigo/dev_sdk/indicators"
    "github.com/kdraigo/dev_sdk/types"
)

s.SetOnCandleFor(types.Timeframe1h, func(ctx *types.Context, c *types.Candle) {
    calc := s.IndicatorManagerFor(types.Timeframe1h)

    rsi, err := calc.RSI("binance", "BTC/USDT", "close", 14)
    if err != nil {
        return // not enough history yet
    }
    latest := rsi[len(rsi)-1] // most recent value is the LAST element
    _ = latest
})
```

Every method has the shape:

```go
IndicatorManagerFor(tf).<Name>(exchange, symbol string, <params...>) (<series...> []float64, error)
```

### Rules

- **Register the timeframe first.** The manager only updates indicators for
  timeframes listed in `Config.Timeframes`. Call `IndicatorManagerFor(tf)` with
  one of those timeframes.
- **Latest value = last element**: `series[len(series)-1]`. The returned slice is
  aligned to candle history; TA-Lib zero-fills the leading lookback region.
- **Error handling**: returns an error when there isn't enough data
  (`len(points) <= period`) or the exchange/symbol is unknown. Treat the error as
  "warm-up not finished" and return early.
- **`pointType`** selects which price series single-input indicators run on:
  `"close"` (default), `"open"`, `"high"`, `"low"`, `"volume"`. Indicators that
  need OHLC / HL / HLC / HLCV derive those internally and take **no** `pointType`.
- **`maType`** (moving-average type) uses the re-exported constants:
  `indicators.TypeSMA`, `TypeEMA`, `TypeWMA`, `TypeDEMA`, `TypeTEMA`, `TypeTRIMA`,
  `TypeKAMA`, `TypeMAMA`, `TypeT3MA`.

In the tables below the leading `exchange, symbol` params are omitted; `pt` =
`pointType`. Unless noted, the return is a single `[]float64` series (plus
`error`).

## Overlap studies / moving averages

| Method | Params | Returns | Description |
|---|---|---|---|
| `BB` | `pt, period int, deviation float64, maType` | upper, middle, lower | Bollinger Bands |
| `DEMA` | `pt, period int` | series | Double Exponential MA |
| `EMA` | `pt, period int` | series | Exponential MA |
| `HTTrendline` | `pt` | series | Hilbert Transform — Instantaneous Trendline |
| `KAMA` | `pt, period int` | series | Kaufman Adaptive MA |
| `MA` | `pt, period int, maType` | series | Generic MA (type selectable) |
| `MAMA` | `pt, fastLimit, slowLimit float64` | mama, fama | MESA Adaptive MA |
| `MaVp` | `pt, periods []float64, minPeriod, maxPeriod int, maType` | series | Variable-period MA |
| `MidPoint` | `pt, period int` | series | Midpoint over period |
| `MidPrice` | `period int` | series | Midpoint price (HL) |
| `SAR` | `acceleration, maximum float64` | series | Parabolic SAR (HL) |
| `SARExt` | `startValue, offsetOnReverse, accelerationInitLong, accelerationLong, accelerationMaxLong, accelerationInitShort, accelerationShort, accelerationMaxShort float64` | series | Extended Parabolic SAR (HL) |
| `SMA` | `pt, period int` | series | Simple MA |
| `T3` | `pt, period int, vFactor float64` | series | Tillson T3 |
| `TEMA` | `pt, period int` | series | Triple Exponential MA |
| `TRIMA` | `pt, period int` | series | Triangular MA |
| `WMA` | `pt, period int` | series | Weighted MA |

## Momentum indicators

| Method | Params | Returns | Description |
|---|---|---|---|
| `ADX` | `period int` | series | Average Directional Index (HLC) |
| `ADXR` | `period int` | series | ADX Rating (HLC) |
| `APO` | `pt, fastPeriod, slowPeriod int, maType` | series | Absolute Price Oscillator |
| `Aroon` | `period int` | aroonDown, aroonUp | Aroon (HL) |
| `AroonOsc` | `period int` | series | Aroon Oscillator (HL) |
| `BOP` | — | series | Balance of Power (OHLC) |
| `CMO` | `pt, period int` | series | Chande Momentum Oscillator |
| `CCI` | `period int` | series | Commodity Channel Index (HLC) |
| `DX` | `period int` | series | Directional Movement Index (HLC) |
| `MACD` | `pt, fastPeriod, slowPeriod, signalPeriod int` | macd, signal, hist | MACD |
| `MACDExt` | `pt, fastPeriod int, fastMAType, slowPeriod int, slowMAType, signalPeriod int, signalMAType` | macd, signal, hist | MACD with selectable MA types |
| `MACDFix` | `pt, signalPeriod int` | macd, signal, hist | MACD fixed 12/26 |
| `MinusDI` | `period int` | series | Minus Directional Indicator (HLC) |
| `MinusDM` | `period int` | series | Minus Directional Movement (HL) |
| `MFI` | `period int` | series | Money Flow Index (HLCV) |
| `Momentum` | `pt, period int` | series | Momentum |
| `PlusDI` | `period int` | series | Plus Directional Indicator (HLC) |
| `PlusDM` | `period int` | series | Plus Directional Movement (HL) |
| `PPO` | `pt, fastPeriod, slowPeriod int, maType` | series | Percentage Price Oscillator |
| `ROC` | `pt, period int` | series | Rate of Change |
| `ROCP` | `pt, period int` | series | Rate of Change Percentage |
| `ROCR` | `pt, period int` | series | Rate of Change Ratio |
| `ROCR100` | `pt, period int` | series | Rate of Change Ratio ×100 |
| `RSI` | `pt, period int` | series | Relative Strength Index |
| `Stoch` | `fastKPeriod, slowKPeriod int, slowKMAType, slowDPeriod int, slowDMAType` | slowK, slowD | Stochastic (HLC) |
| `StochF` | `fastKPeriod, fastDPeriod int, fastDMAType` | fastK, fastD | Stochastic Fast (HLC) |
| `StochRSI` | `pt, period, fastKPeriod, fastDPeriod int, fastDMAType` | fastK, fastD | Stochastic RSI |
| `Trix` | `pt, period int` | series | TRIX |
| `UltOsc` | `period1, period2, period3 int` | series | Ultimate Oscillator (HLC) |
| `WilliamsR` | `period int` | series | Williams %R (HLC) |

## Volume indicators

| Method | Params | Returns | Description |
|---|---|---|---|
| `Ad` | — | series | Chaikin A/D Line (HLCV) |
| `AdOsc` | `fastPeriod, slowPeriod int` | series | Chaikin A/D Oscillator (HLCV) |
| `OBV` | `pt` | series | On Balance Volume (price + volume) |

## Volatility indicators

| Method | Params | Returns | Description |
|---|---|---|---|
| `ATR` | `period int` | series | Average True Range (HLC) |
| `NATR` | `period int` | series | Normalized ATR (HLC) |
| `TRANGE` | — | series | True Range (HLC) |

## Price transform

| Method | Params | Returns | Description |
|---|---|---|---|
| `AvgPrice` | — | series | (O+H+L+C)/4 |
| `MedPrice` | — | series | (H+L)/2 |
| `TypPrice` | — | series | (H+L+C)/3 |
| `WCLPrice` | — | series | (H+L+2C)/4 |

## Cycle indicators (Hilbert Transform)

| Method | Params | Returns | Description |
|---|---|---|---|
| `HTDcPeriod` | `pt` | series | Dominant Cycle Period |
| `HTDcPhase` | `pt` | series | Dominant Cycle Phase |
| `HTPhasor` | `pt` | inPhase, quadrature | Phasor Components |
| `HTSine` | `pt` | sine, leadSine | SineWave |
| `HTTrendMode` | `pt` | series | Trend vs Cycle Mode (0/1) |

## Statistic functions

| Method | Params | Returns | Description |
|---|---|---|---|
| `Beta` | `pt0, pt1, period int` | series | Beta of pt0 vs pt1 |
| `Correl` | `pt0, pt1, period int` | series | Pearson Correlation |
| `LinearReg` | `pt, period int` | series | Linear Regression |
| `LinearRegAngle` | `pt, period int` | series | LinReg Angle |
| `LinearRegIntercept` | `pt, period int` | series | LinReg Intercept |
| `LinearRegSlope` | `pt, period int` | series | LinReg Slope |
| `StdDev` | `pt, period int, nbDev float64` | series | Standard Deviation |
| `TSF` | `pt, period int` | series | Time Series Forecast |
| `Var` | `pt, period int` | series | Variance |

## Math transform (element-wise)

Each takes `pt` only and returns a series: `Acos`, `Asin`, `Atan`, `Ceil`,
`Cos`, `Cosh`, `Exp`, `Floor`, `Ln`, `Log10`, `Sin`, `Sinh`, `Sqrt`, `Tan`,
`Tanh`.

## Math operators

| Method | Params | Returns | Description |
|---|---|---|---|
| `Add` | `pt0, pt1` | series | pt0 + pt1 |
| `Sub` | `pt0, pt1` | series | pt0 − pt1 |
| `Mult` | `pt0, pt1` | series | pt0 × pt1 |
| `Div` | `pt0, pt1` | series | pt0 ÷ pt1 |
| `Max` | `pt, period int` | series | Highest value over period |
| `Min` | `pt, period int` | series | Lowest value over period |
| `MaxIndex` | `pt, period int` | series | Index of highest over period |
| `MinIndex` | `pt, period int` | series | Index of lowest over period |
| `MinMax` | `pt, period int` | min, max | Lowest & highest over period |
| `MinMaxIndex` | `pt, period int` | minIdx, maxIdx | Indices of lowest & highest |
| `Sum` | `pt, period int` | series | Rolling sum |

## Multi-output example (MACD)

```go
calc := s.IndicatorManagerFor(types.Timeframe1h)

macd, signal, hist, err := calc.MACD("binance", "BTC/USDT", "close", 12, 26, 9)
if err != nil {
    return
}
i := len(macd) - 1
if macd[i] > signal[i] && hist[i] > 0 {
    // bullish crossover
}
```

## MA-type example (Bollinger Bands on an EMA basis)

```go
upper, mid, lower, err := calc.BB("binance", "BTC/USDT", "close", 20, 2.0, indicators.TypeEMA)
```
