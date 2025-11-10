package de.berlin.htw.trading.candle.dto;

public record Candle(
                long bucketStartSec,
                double open,
                double high,
                double low,
                double close,
                long ticks,
                Double precision) {
}
