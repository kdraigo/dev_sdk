package indicators

type IndicatorsCalculatorGen interface {
	BB(exchange string, symbol string, pointType string, period int, deviation float64, maType MaType) ([]float64, []float64, []float64, error)
	DEMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	EMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	HTTrendline(exchange string, symbol string, pointType string) ([]float64, error)
	KAMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	MA(exchange string, symbol string, pointType string, period int, maType MaType) ([]float64, error)
	MAMA(exchange string, symbol string, pointType string, fastLimit float64, slowLimit float64) ([]float64, []float64, error)
	MaVp(exchange string, symbol string, pointType string, periods []float64, minPeriod int, maxPeriod int, maType MaType) ([]float64, error)
	MidPoint(exchange string, symbol string, pointType string, period int) ([]float64, error)
	MidPrice(exchange string, symbol string, period int) ([]float64, error)
	SAR(exchange string, symbol string, acceleration float64, maximum float64) ([]float64, error)
	SARExt(exchange string, symbol string, startValue float64, offsetOnReverse float64, accelerationInitLong float64, accelerationLong float64, accelerationMaxLong float64, accelerationInitShort float64, accelerationShort float64, accelerationMaxShort float64) ([]float64, error)
	SMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	T3(exchange string, symbol string, pointType string, period int, vFactor float64) ([]float64, error)
	TEMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	TRIMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	WMA(exchange string, symbol string, pointType string, period int) ([]float64, error)
	ADX(exchange string, symbol string, period int) ([]float64, error)
	ADXR(exchange string, symbol string, period int) ([]float64, error)
	APO(exchange string, symbol string, pointType string, fastPeriod int, slowPeriod int, maType MaType) ([]float64, error)
	Aroon(exchange string, symbol string, period int) ([]float64, []float64, error)
	AroonOsc(exchange string, symbol string, period int) ([]float64, error)
	BOP(exchange string, symbol string) ([]float64, error)
	CMO(exchange string, symbol string, pointType string, period int) ([]float64, error)
	CCI(exchange string, symbol string, period int) ([]float64, error)
	DX(exchange string, symbol string, period int) ([]float64, error)
	MACD(exchange string, symbol string, pointType string, fastPeriod int, slowPeriod int, signalPeriod int) ([]float64, []float64, []float64, error)
	MACDExt(exchange string, symbol string, pointType string, fastPeriod int, fastMAType MaType, slowPeriod int, slowMAType MaType, signalPeriod int, signalMAType MaType) ([]float64, []float64, []float64, error)
	MACDFix(exchange string, symbol string, pointType string, signalPeriod int) ([]float64, []float64, []float64, error)
	MinusDI(exchange string, symbol string, period int) ([]float64, error)
	MinusDM(exchange string, symbol string, period int) ([]float64, error)
	MFI(exchange string, symbol string, period int) ([]float64, error)
	Momentum(exchange string, symbol string, pointType string, period int) ([]float64, error)
	PlusDI(exchange string, symbol string, period int) ([]float64, error)
	PlusDM(exchange string, symbol string, period int) ([]float64, error)
	PPO(exchange string, symbol string, pointType string, fastPeriod int, slowPeriod int, maType MaType) ([]float64, error)
	ROCP(exchange string, symbol string, pointType string, period int) ([]float64, error)
	ROC(exchange string, symbol string, pointType string, period int) ([]float64, error)
	ROCR(exchange string, symbol string, pointType string, period int) ([]float64, error)
	ROCR100(exchange string, symbol string, pointType string, period int) ([]float64, error)
	RSI(exchange string, symbol string, pointType string, period int) ([]float64, error)
	Stoch(exchange string, symbol string, fastKPeriod int, slowKPeriod int, slowKMAType MaType, slowDPeriod int, slowDMAType MaType) ([]float64, []float64, error)
	StochF(exchange string, symbol string, fastKPeriod int, fastDPeriod int, fastDMAType MaType) ([]float64, []float64, error)
	StochRSI(exchange string, symbol string, pointType string, period int, fastKPeriod int, fastDPeriod int, fastDMAType MaType) ([]float64, []float64, error)
	Trix(exchange string, symbol string, pointType string, period int) ([]float64, error)
	UltOsc(exchange string, symbol string, period1 int, period2 int, period3 int) ([]float64, error)
	WilliamsR(exchange string, symbol string, period int) ([]float64, error)
	Ad(exchange string, symbol string) ([]float64, error)
	AdOsc(exchange string, symbol string, fastPeriod int, slowPeriod int) ([]float64, error)
	OBV(exchange string, symbol string, pointType string) ([]float64, error)
	ATR(exchange string, symbol string, period int) ([]float64, error)
	NATR(exchange string, symbol string, period int) ([]float64, error)
	TRANGE(exchange string, symbol string) ([]float64, error)
	AvgPrice(exchange string, symbol string) ([]float64, error)
	MedPrice(exchange string, symbol string) ([]float64, error)
	TypPrice(exchange string, symbol string) ([]float64, error)
	WCLPrice(exchange string, symbol string) ([]float64, error)
	HTDcPeriod(exchange string, symbol string, pointType string) ([]float64, error)
	HTDcPhase(exchange string, symbol string, pointType string) ([]float64, error)
	HTPhasor(exchange string, symbol string, pointType string) ([]float64, []float64, error)
	HTSine(exchange string, symbol string, pointType string) ([]float64, []float64, error)
	HTTrendMode(exchange string, symbol string, pointType string) ([]float64, error)
	Beta(exchange string, symbol string, pointType0 string, pointType1 string, period int) ([]float64, error)
	Correl(exchange string, symbol string, pointType0 string, pointType1 string, period int) ([]float64, error)
	LinearReg(exchange string, symbol string, pointType string, period int) ([]float64, error)
	LinearRegAngle(exchange string, symbol string, pointType string, period int) ([]float64, error)
	LinearRegIntercept(exchange string, symbol string, pointType string, period int) ([]float64, error)
	LinearRegSlope(exchange string, symbol string, pointType string, period int) ([]float64, error)
	StdDev(exchange string, symbol string, pointType string, period int, nbDev float64) ([]float64, error)
	TSF(exchange string, symbol string, pointType string, period int) ([]float64, error)
	Var(exchange string, symbol string, pointType string, period int) ([]float64, error)
	Acos(exchange string, symbol string, pointType string) ([]float64, error)
	Asin(exchange string, symbol string, pointType string) ([]float64, error)
	Atan(exchange string, symbol string, pointType string) ([]float64, error)
	Ceil(exchange string, symbol string, pointType string) ([]float64, error)
	Cos(exchange string, symbol string, pointType string) ([]float64, error)
	Cosh(exchange string, symbol string, pointType string) ([]float64, error)
	Exp(exchange string, symbol string, pointType string) ([]float64, error)
	Floor(exchange string, symbol string, pointType string) ([]float64, error)
	Ln(exchange string, symbol string, pointType string) ([]float64, error)
	Log10(exchange string, symbol string, pointType string) ([]float64, error)
	Sin(exchange string, symbol string, pointType string) ([]float64, error)
	Sinh(exchange string, symbol string, pointType string) ([]float64, error)
	Sqrt(exchange string, symbol string, pointType string) ([]float64, error)
	Tan(exchange string, symbol string, pointType string) ([]float64, error)
	Tanh(exchange string, symbol string, pointType string) ([]float64, error)
	Add(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error)
	Div(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error)
	Max(exchange string, symbol string, pointType string, period int) ([]float64, error)
	MaxIndex(exchange string, symbol string, pointType string, period int) ([]float64, error)
	Min(exchange string, symbol string, pointType string, period int) ([]float64, error)
	MinIndex(exchange string, symbol string, pointType string, period int) ([]float64, error)
	MinMax(exchange string, symbol string, pointType string, period int) ([]float64, []float64, error)
	MinMaxIndex(exchange string, symbol string, pointType string, period int) ([]float64, []float64, error)
	Mult(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error)
	Sub(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error)
	Sum(exchange string, symbol string, pointType string, period int) ([]float64, error)
}

func (im *indicatorManager) BB(exchange string, symbol string, pointType string, period int, deviation float64, maType MaType) ([]float64, []float64, []float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, nil, err
	}

	res1, res2, res3 := BB(points, period, deviation, maType)
	return res1, res2, res3, nil
}

func (im *indicatorManager) DEMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := DEMA(points, period)
	return res, nil
}

func (im *indicatorManager) EMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := EMA(points, period)
	return res, nil
}

func (im *indicatorManager) HTTrendline(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := HTTrendline(points)
	return res, nil
}

func (im *indicatorManager) KAMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := KAMA(points, period)
	return res, nil
}

func (im *indicatorManager) MA(exchange string, symbol string, pointType string, period int, maType MaType) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MA(points, period, maType)
	return res, nil
}

func (im *indicatorManager) MAMA(exchange string, symbol string, pointType string, fastLimit float64, slowLimit float64) ([]float64, []float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := MAMA(points, fastLimit, slowLimit)
	return res1, res2, nil
}

func (im *indicatorManager) MaVp(exchange string, symbol string, pointType string, periods []float64, minPeriod int, maxPeriod int, maType MaType) ([]float64, error) {
	reqPeriod := minPeriod
	if maxPeriod > reqPeriod {
		reqPeriod = maxPeriod
	}

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MaVp(points, periods, minPeriod, maxPeriod, maType)
	return res, nil
}

func (im *indicatorManager) MidPoint(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MidPoint(points, period)
	return res, nil
}

func (im *indicatorManager) MidPrice(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MidPrice(high, low, period)
	return res, nil
}

func (im *indicatorManager) SAR(exchange string, symbol string, acceleration float64, maximum float64) ([]float64, error) {
	reqPeriod := 1
	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := SAR(high, low, acceleration, maximum)
	return res, nil
}

func (im *indicatorManager) SARExt(exchange string, symbol string, startValue float64, offsetOnReverse float64, accelerationInitLong float64, accelerationLong float64, accelerationMaxLong float64, accelerationInitShort float64, accelerationShort float64, accelerationMaxShort float64) ([]float64, error) {
	reqPeriod := 1
	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := SARExt(high, low, startValue, offsetOnReverse, accelerationInitLong, accelerationLong, accelerationMaxLong, accelerationInitShort, accelerationShort, accelerationMaxShort)
	return res, nil
}

func (im *indicatorManager) SMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := SMA(points, period)
	return res, nil
}

func (im *indicatorManager) T3(exchange string, symbol string, pointType string, period int, vFactor float64) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := T3(points, period, vFactor)
	return res, nil
}

func (im *indicatorManager) TEMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := TEMA(points, period)
	return res, nil
}

func (im *indicatorManager) TRIMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := TRIMA(points, period)
	return res, nil
}

func (im *indicatorManager) WMA(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := WMA(points, period)
	return res, nil
}

func (im *indicatorManager) ADX(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ADX(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) ADXR(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ADXR(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) APO(exchange string, symbol string, pointType string, fastPeriod int, slowPeriod int, maType MaType) ([]float64, error) {
	reqPeriod := fastPeriod
	if slowPeriod > reqPeriod {
		reqPeriod = slowPeriod
	}

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := APO(points, fastPeriod, slowPeriod, maType)
	return res, nil
}

func (im *indicatorManager) Aroon(exchange string, symbol string, period int) ([]float64, []float64, error) {
	reqPeriod := period

	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := Aroon(high, low, period)
	return res1, res2, nil
}

func (im *indicatorManager) AroonOsc(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := AroonOsc(high, low, period)
	return res, nil
}

func (im *indicatorManager) BOP(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	open, high, low, closePrices, err := im.getOHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := BOP(open, high, low, closePrices)
	return res, nil
}

func (im *indicatorManager) CMO(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := CMO(points, period)
	return res, nil
}

func (im *indicatorManager) CCI(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := CCI(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) DX(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := DX(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) MACD(exchange string, symbol string, pointType string, fastPeriod int, slowPeriod int, signalPeriod int) ([]float64, []float64, []float64, error) {
	reqPeriod := fastPeriod
	if slowPeriod > reqPeriod {
		reqPeriod = slowPeriod
	}
	if signalPeriod > reqPeriod {
		reqPeriod = signalPeriod
	}

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, nil, err
	}

	res1, res2, res3 := MACD(points, fastPeriod, slowPeriod, signalPeriod)
	return res1, res2, res3, nil
}

func (im *indicatorManager) MACDExt(exchange string, symbol string, pointType string, fastPeriod int, fastMAType MaType, slowPeriod int, slowMAType MaType, signalPeriod int, signalMAType MaType) ([]float64, []float64, []float64, error) {
	reqPeriod := fastPeriod
	if slowPeriod > reqPeriod {
		reqPeriod = slowPeriod
	}
	if signalPeriod > reqPeriod {
		reqPeriod = signalPeriod
	}

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, nil, err
	}

	res1, res2, res3 := MACDExt(points, fastPeriod, fastMAType, slowPeriod, slowMAType, signalPeriod, signalMAType)
	return res1, res2, res3, nil
}

func (im *indicatorManager) MACDFix(exchange string, symbol string, pointType string, signalPeriod int) ([]float64, []float64, []float64, error) {
	reqPeriod := signalPeriod

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, nil, err
	}

	res1, res2, res3 := MACDFix(points, signalPeriod)
	return res1, res2, res3, nil
}

func (im *indicatorManager) MinusDI(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MinusDI(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) MinusDM(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MinusDM(high, low, period)
	return res, nil
}

func (im *indicatorManager) MFI(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, volume, err := im.getHLCV(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MFI(high, low, closePrices, volume, period)
	return res, nil
}

func (im *indicatorManager) Momentum(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Momentum(points, period)
	return res, nil
}

func (im *indicatorManager) PlusDI(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := PlusDI(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) PlusDM(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := PlusDM(high, low, period)
	return res, nil
}

func (im *indicatorManager) PPO(exchange string, symbol string, pointType string, fastPeriod int, slowPeriod int, maType MaType) ([]float64, error) {
	reqPeriod := fastPeriod
	if slowPeriod > reqPeriod {
		reqPeriod = slowPeriod
	}

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := PPO(points, fastPeriod, slowPeriod, maType)
	return res, nil
}

func (im *indicatorManager) ROCP(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ROCP(points, period)
	return res, nil
}

func (im *indicatorManager) ROC(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ROC(points, period)
	return res, nil
}

func (im *indicatorManager) ROCR(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ROCR(points, period)
	return res, nil
}

func (im *indicatorManager) ROCR100(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ROCR100(points, period)
	return res, nil
}

func (im *indicatorManager) RSI(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := RSI(points, period)
	return res, nil
}

func (im *indicatorManager) Stoch(exchange string, symbol string, fastKPeriod int, slowKPeriod int, slowKMAType MaType, slowDPeriod int, slowDMAType MaType) ([]float64, []float64, error) {
	reqPeriod := fastKPeriod
	if slowKPeriod > reqPeriod {
		reqPeriod = slowKPeriod
	}
	if slowDPeriod > reqPeriod {
		reqPeriod = slowDPeriod
	}

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := Stoch(high, low, closePrices, fastKPeriod, slowKPeriod, slowKMAType, slowDPeriod, slowDMAType)
	return res1, res2, nil
}

func (im *indicatorManager) StochF(exchange string, symbol string, fastKPeriod int, fastDPeriod int, fastDMAType MaType) ([]float64, []float64, error) {
	reqPeriod := fastKPeriod
	if fastDPeriod > reqPeriod {
		reqPeriod = fastDPeriod
	}

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := StochF(high, low, closePrices, fastKPeriod, fastDPeriod, fastDMAType)
	return res1, res2, nil
}

func (im *indicatorManager) StochRSI(exchange string, symbol string, pointType string, period int, fastKPeriod int, fastDPeriod int, fastDMAType MaType) ([]float64, []float64, error) {
	reqPeriod := period
	if fastKPeriod > reqPeriod {
		reqPeriod = fastKPeriod
	}
	if fastDPeriod > reqPeriod {
		reqPeriod = fastDPeriod
	}

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := StochRSI(points, period, fastKPeriod, fastDPeriod, fastDMAType)
	return res1, res2, nil
}

func (im *indicatorManager) Trix(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Trix(points, period)
	return res, nil
}

func (im *indicatorManager) UltOsc(exchange string, symbol string, period1 int, period2 int, period3 int) ([]float64, error) {
	reqPeriod := period1
	if period2 > reqPeriod {
		reqPeriod = period2
	}
	if period3 > reqPeriod {
		reqPeriod = period3
	}

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := UltOsc(high, low, closePrices, period1, period2, period3)
	return res, nil
}

func (im *indicatorManager) WilliamsR(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := WilliamsR(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) Ad(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	high, low, closePrices, volume, err := im.getHLCV(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Ad(high, low, closePrices, volume)
	return res, nil
}

func (im *indicatorManager) AdOsc(exchange string, symbol string, fastPeriod int, slowPeriod int) ([]float64, error) {
	reqPeriod := fastPeriod
	if slowPeriod > reqPeriod {
		reqPeriod = slowPeriod
	}

	high, low, closePrices, volume, err := im.getHLCV(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := AdOsc(high, low, closePrices, volume, fastPeriod, slowPeriod)
	return res, nil
}

func (im *indicatorManager) OBV(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}
	_, _, _, _, volume, err := im.getOHLCV(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := OBV(points, volume)
	return res, nil
}

func (im *indicatorManager) ATR(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := ATR(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) NATR(exchange string, symbol string, period int) ([]float64, error) {
	reqPeriod := period

	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := NATR(high, low, closePrices, period)
	return res, nil
}

func (im *indicatorManager) TRANGE(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := TRANGE(high, low, closePrices)
	return res, nil
}

func (im *indicatorManager) AvgPrice(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	open, high, low, closePrices, err := im.getOHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := AvgPrice(open, high, low, closePrices)
	return res, nil
}

func (im *indicatorManager) MedPrice(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	high, low, err := im.getHL(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MedPrice(high, low)
	return res, nil
}

func (im *indicatorManager) TypPrice(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := TypPrice(high, low, closePrices)
	return res, nil
}

func (im *indicatorManager) WCLPrice(exchange string, symbol string) ([]float64, error) {
	reqPeriod := 1
	high, low, closePrices, err := im.getHLC(exchange, symbol, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := WCLPrice(high, low, closePrices)
	return res, nil
}

func (im *indicatorManager) HTDcPeriod(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := HTDcPeriod(points)
	return res, nil
}

func (im *indicatorManager) HTDcPhase(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := HTDcPhase(points)
	return res, nil
}

func (im *indicatorManager) HTPhasor(exchange string, symbol string, pointType string) ([]float64, []float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := HTPhasor(points)
	return res1, res2, nil
}

func (im *indicatorManager) HTSine(exchange string, symbol string, pointType string) ([]float64, []float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := HTSine(points)
	return res1, res2, nil
}

func (im *indicatorManager) HTTrendMode(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := HTTrendMode(points)
	return res, nil
}

func (im *indicatorManager) Beta(exchange string, symbol string, pointType0 string, pointType1 string, period int) ([]float64, error) {
	reqPeriod := period

	in0, err := im.getPoints(exchange, symbol, pointType0, reqPeriod)
	if err != nil {
		return nil, err
	}
	in1, err := im.getPoints(exchange, symbol, pointType1, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Beta(in0, in1, period)
	return res, nil
}

func (im *indicatorManager) Correl(exchange string, symbol string, pointType0 string, pointType1 string, period int) ([]float64, error) {
	reqPeriod := period

	in0, err := im.getPoints(exchange, symbol, pointType0, reqPeriod)
	if err != nil {
		return nil, err
	}
	in1, err := im.getPoints(exchange, symbol, pointType1, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Correl(in0, in1, period)
	return res, nil
}

func (im *indicatorManager) LinearReg(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := LinearReg(points, period)
	return res, nil
}

func (im *indicatorManager) LinearRegAngle(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := LinearRegAngle(points, period)
	return res, nil
}

func (im *indicatorManager) LinearRegIntercept(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := LinearRegIntercept(points, period)
	return res, nil
}

func (im *indicatorManager) LinearRegSlope(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := LinearRegSlope(points, period)
	return res, nil
}

func (im *indicatorManager) StdDev(exchange string, symbol string, pointType string, period int, nbDev float64) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := StdDev(points, period, nbDev)
	return res, nil
}

func (im *indicatorManager) TSF(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := TSF(points, period)
	return res, nil
}

func (im *indicatorManager) Var(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Var(points, period)
	return res, nil
}

func (im *indicatorManager) Acos(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Acos(points)
	return res, nil
}

func (im *indicatorManager) Asin(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Asin(points)
	return res, nil
}

func (im *indicatorManager) Atan(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Atan(points)
	return res, nil
}

func (im *indicatorManager) Ceil(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Ceil(points)
	return res, nil
}

func (im *indicatorManager) Cos(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Cos(points)
	return res, nil
}

func (im *indicatorManager) Cosh(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Cosh(points)
	return res, nil
}

func (im *indicatorManager) Exp(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Exp(points)
	return res, nil
}

func (im *indicatorManager) Floor(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Floor(points)
	return res, nil
}

func (im *indicatorManager) Ln(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Ln(points)
	return res, nil
}

func (im *indicatorManager) Log10(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Log10(points)
	return res, nil
}

func (im *indicatorManager) Sin(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Sin(points)
	return res, nil
}

func (im *indicatorManager) Sinh(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Sinh(points)
	return res, nil
}

func (im *indicatorManager) Sqrt(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Sqrt(points)
	return res, nil
}

func (im *indicatorManager) Tan(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Tan(points)
	return res, nil
}

func (im *indicatorManager) Tanh(exchange string, symbol string, pointType string) ([]float64, error) {
	reqPeriod := 1
	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Tanh(points)
	return res, nil
}

func (im *indicatorManager) Add(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error) {
	reqPeriod := 1
	in0, err := im.getPoints(exchange, symbol, pointType0, reqPeriod)
	if err != nil {
		return nil, err
	}
	in1, err := im.getPoints(exchange, symbol, pointType1, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Add(in0, in1)
	return res, nil
}

func (im *indicatorManager) Div(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error) {
	reqPeriod := 1
	in0, err := im.getPoints(exchange, symbol, pointType0, reqPeriod)
	if err != nil {
		return nil, err
	}
	in1, err := im.getPoints(exchange, symbol, pointType1, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Div(in0, in1)
	return res, nil
}

func (im *indicatorManager) Max(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Max(points, period)
	return res, nil
}

func (im *indicatorManager) MaxIndex(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MaxIndex(points, period)
	return res, nil
}

func (im *indicatorManager) Min(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Min(points, period)
	return res, nil
}

func (im *indicatorManager) MinIndex(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := MinIndex(points, period)
	return res, nil
}

func (im *indicatorManager) MinMax(exchange string, symbol string, pointType string, period int) ([]float64, []float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := MinMax(points, period)
	return res1, res2, nil
}

func (im *indicatorManager) MinMaxIndex(exchange string, symbol string, pointType string, period int) ([]float64, []float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, nil, err
	}

	res1, res2 := MinMaxIndex(points, period)
	return res1, res2, nil
}

func (im *indicatorManager) Mult(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error) {
	reqPeriod := 1
	in0, err := im.getPoints(exchange, symbol, pointType0, reqPeriod)
	if err != nil {
		return nil, err
	}
	in1, err := im.getPoints(exchange, symbol, pointType1, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Mult(in0, in1)
	return res, nil
}

func (im *indicatorManager) Sub(exchange string, symbol string, pointType0 string, pointType1 string) ([]float64, error) {
	reqPeriod := 1
	in0, err := im.getPoints(exchange, symbol, pointType0, reqPeriod)
	if err != nil {
		return nil, err
	}
	in1, err := im.getPoints(exchange, symbol, pointType1, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Sub(in0, in1)
	return res, nil
}

func (im *indicatorManager) Sum(exchange string, symbol string, pointType string, period int) ([]float64, error) {
	reqPeriod := period

	points, err := im.getPoints(exchange, symbol, pointType, reqPeriod)
	if err != nil {
		return nil, err
	}

	res := Sum(points, period)
	return res, nil
}
