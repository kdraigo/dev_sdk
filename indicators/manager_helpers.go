package indicators

import "errors"

func (im *indicatorManager) getPoints(exchange, symbol, pointType string, requiredPeriod int) ([]float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, errors.New("invalid symbol")
	}

	points := symbolPoints.Close
	if pointType == "high" {
		points = symbolPoints.High
	} else if pointType == "low" {
		points = symbolPoints.Low
	} else if pointType == "open" {
		points = symbolPoints.Open
	} else if pointType == "volume" {
		points = symbolPoints.Volume
	}

	if len(points) <= requiredPeriod {
		return nil, errors.New("not enough data for indicator calculation")
	}

	return points, nil
}

func (im *indicatorManager) getOHLC(exchange, symbol string, requiredPeriod int) ([]float64, []float64, []float64, []float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, nil, nil, nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, nil, nil, nil, errors.New("invalid symbol")
	}

	if len(symbolPoints.Close) <= requiredPeriod {
		return nil, nil, nil, nil, errors.New("not enough data for indicator calculation")
	}

	return symbolPoints.Open, symbolPoints.High, symbolPoints.Low, symbolPoints.Close, nil
}

func (im *indicatorManager) getHLC(exchange, symbol string, requiredPeriod int) ([]float64, []float64, []float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, nil, nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, nil, nil, errors.New("invalid symbol")
	}

	if len(symbolPoints.Close) <= requiredPeriod {
		return nil, nil, nil, errors.New("not enough data for indicator calculation")
	}

	return symbolPoints.High, symbolPoints.Low, symbolPoints.Close, nil
}

func (im *indicatorManager) getHL(exchange, symbol string, requiredPeriod int) ([]float64, []float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, nil, errors.New("invalid symbol")
	}

	if len(symbolPoints.Close) <= requiredPeriod {
		return nil, nil, errors.New("not enough data for indicator calculation")
	}

	return symbolPoints.High, symbolPoints.Low, nil
}

func (im *indicatorManager) getHLCV(exchange, symbol string, requiredPeriod int) ([]float64, []float64, []float64, []float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, nil, nil, nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, nil, nil, nil, errors.New("invalid symbol")
	}

	if len(symbolPoints.Close) <= requiredPeriod {
		return nil, nil, nil, nil, errors.New("not enough data for indicator calculation")
	}

	return symbolPoints.High, symbolPoints.Low, symbolPoints.Close, symbolPoints.Volume, nil
}

func (im *indicatorManager) getOHLCV(exchange, symbol string, requiredPeriod int) ([]float64, []float64, []float64, []float64, []float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, nil, nil, nil, nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, nil, nil, nil, nil, errors.New("invalid symbol")
	}

	if len(symbolPoints.Close) <= requiredPeriod {
		return nil, nil, nil, nil, nil, errors.New("not enough data for indicator calculation")
	}

	return symbolPoints.Open, symbolPoints.High, symbolPoints.Low, symbolPoints.Close, symbolPoints.Volume, nil
}
