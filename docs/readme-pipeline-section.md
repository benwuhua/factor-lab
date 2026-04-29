## Platform Loop

Factor Lab is an AI-assisted A-share research workbench. It is built around one conservative loop: govern the data first, research factors inside bounded lanes, combine approved factor families, then pass every portfolio through review gates before paper execution.

![Factor Lab commercial research pipeline](docs/assets/factor-lab-commercial-pipeline.svg)

The diagram emphasizes the current operating model:

- Data: CSI300 and CSI500 research universes, OHLCV, security master, company events, point-in-time governance, freshness and coverage gates.
- Factors: hand-built candidates, JoinQuant migrations, strategy-dictionary seeds, and multi-lane autoresearch across price-volume, pattern, emotion, liquidity, risk, fundamental, shareholder, theme, and regime lanes.
- Portfolio: purified factor values, representative factors by family, target portfolio generation, concentration caps, factor drivers, and exposure attribution.
- Review and replay: portfolio gate, expert review packet, stock cards, event-risk blocks, paper orders, simulated fills, reconciliation, and replay validation.

