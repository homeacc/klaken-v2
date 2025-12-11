# INSTITUTIONAL REGIMES - Trading System

Documento de referencia basado en backtests de 3 años (Dic 2022 - Dic 2025).
Datos: Hyblock Capital API v2 (top_traders, funding_rate, whale_retail_delta).

---

## RESUMEN EJECUTIVO

| Activo | Mejor Señal | Horizonte | PF | Frecuencia |
|--------|-------------|-----------|-----|------------|
| SOL | LONG: L/S <2.0 + whale>0 + funding<0 | 4h | 3.96 | ~1/semana |
| BTC | LONG: L/S 2.0-3.0 + funding<0.05% | 24h | 6.28 | ~1/5 días |
| ETH | LONG: L/S <2.0 + funding<0 | 24h | 2.79 | ~1/día |
| ETH | LONG: L/S 2.0-3.0 + whale>0 + funding<0.2% | 24h | 3.11 | ~1/semana |
| ETH | SHORT: L/S >4.0 | 24h | 1.56 | ~3/semana |

---

## SOL/USD

### Características
- Horizonte óptimo: **4h** (mercado rápido)
- L/S ratio promedio: 1.85
- L/S ratio máximo observado: 3.94
- Frecuencia L/S >2.0: 37%

### Señales LONG

| Prioridad | Condición | PF | WR | Señales/3y |
|-----------|-----------|-----|-----|------------|
| 1 | L/S <2.0 + whale>0 + funding<0 | 3.96 | 58% | 60 |

**NOTA**: P2 (L/S 2.0-3.0) eliminada - backtest mostró PF 0.98 (pierde dinero).

### NO OPERAR
- L/S > 3.0 → PF 0.38 (LONG pierde), PF 0.71 (SHORT pierde)
- Mercado impredecible en consenso extremo

### Parámetros Sugeridos
- Entry: Precio actual cuando se cumple condición
- Target: +3-4%
- Stop Loss: -3%
- Tiempo máximo: 4h

---

## BTC/USD

### Características
- Horizonte óptimo: **24h** (mercado lento)
- L/S ratio promedio: 1.46
- L/S ratio máximo observado: 2.82
- Frecuencia L/S >2.0: 8.3%

### Señales LONG

| Prioridad | Condición | PF | WR | Señales/3y |
|-----------|-----------|-----|-----|------------|
| 1 | L/S 2.0-3.0 + funding<0.05% | 6.28 | 60% | 206 |
| 2 | L/S <2.0 + whale>0 + funding<0 | 2.10 | 57% | 1,071 |

### NO OPERAR
- L/S > 2.8 → Casi nunca ocurre, sin data suficiente
- SHORT no funciona en ningún escenario (PF <1.0)

### Parámetros Sugeridos
- Entry: Precio actual cuando se cumple condición
- Target: +4-5%
- Stop Loss: -3%
- Tiempo máximo: 24h

---

## ETH/USD

### Características
- Horizonte óptimo: **24h** (mercado lento)
- L/S ratio promedio: 2.13 (más long-biased)
- L/S ratio máximo observado: 6.16
- Frecuencia L/S >2.0: 49.5%

### Señales LONG

| Prioridad | Condición | PF | WR | Señales/3y |
|-----------|-----------|-----|-----|------------|
| 1 | L/S <2.0 + funding<0 | 2.79 | 63% | 1,214 |
| 2 | L/S 2.0-3.0 + whale>0 | 1.91 | 58% | 380 |

### Señales SHORT (único activo con SHORT válido)

| Condición | PF | WR | Señales/3y |
|-----------|-----|-----|------------|
| L/S > 4.0 | 1.56 | 54% | 831 |

### NO OPERAR
- L/S 3.0-4.0 → Zona gris, edge débil

### Parámetros Sugeridos LONG
- Target: +4-5%
- Stop Loss: -3%
- Tiempo máximo: 24h

### Parámetros Sugeridos SHORT
- Target: +3-4%
- Stop Loss: -4%
- Tiempo máximo: 24h

---

## FILTROS CRÍTICOS

### whale_retail_delta
- **>0**: Whales comprando (confirmación alcista)
- **>5**: Señal fuerte
- **>10**: Señal muy fuerte (PF sube a 3.89 en SOL)
- **<0**: Whales vendiendo (evitar LONG)

### funding_rate
- **<0**: Funding negativo, mercado pagando shorts (alcista)
- **<0.05%**: Neutral-bajo, aceptable para LONG
- **>0.05%**: Funding alto, mercado crowded long (cautela)

### long_short_ratio (L/S)
- Mide posicionamiento de top 20% traders de Binance por margen
- **<2.0**: Profesionales menos long de lo normal → oportunidad
- **2.0-3.0**: Profesionales moderadamente long → setup estándar
- **>3.0**: Demasiado consenso → evitar o contrarian (solo ETH >4.0)

---

## REGLAS DE ORO

1. **Whale delta es el filtro más importante** - Sin whales comprando, no hay edge
2. **Respetar el horizonte** - SOL es 4h, BTC/ETH son 24h
3. **L/S extremo = peligro** - Excepto ETH SHORT cuando >4.0
4. **SHORT casi nunca funciona** - Solo ETH en extremo
5. **No forzar trades** - Si no hay señal, no hay señal

---

## INTERPRETACIÓN DE /status

Cuando consultes /status de Klaken V2, evaluar:

```
hyblock.top_traders.lsRatio → L/S ratio actual
hyblock.funding_rate.fundingRate → Funding actual
hyblock.whale_retail_delta.whaleRetailDelta → Delta whales actual
```

Ejemplo de evaluación:
```
SOL: L/S=1.8, whale=+3.2, funding=0.0003

Checklist:
✓ L/S <2.0 (1.8)
✓ whale >0 (3.2)
✓ funding <0.05% (0.03%)

→ SEÑAL LONG VÁLIDA (Prioridad 1, PF 2.72)
→ Horizonte: 4h
→ Target: +3-4%
→ SL: -3%
```

---

## CHANGELOG

- 2025-12-11: Versión inicial basada en backtests SOL/BTC/ETH