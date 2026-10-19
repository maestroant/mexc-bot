# Блок-схема стратегии торговли листингами MEXC (по ТЗ v2.1)

Источник: `logika_torgovli_listingov_mexc_v2_1_revised.pdf`. Где разделы ТЗ противоречат друг другу, используется v2.1 (разделы 10–22).

Пометки **⚠ Вn** ссылаются на найденные логические проблемы и открытые вопросы в конце документа.

> Просмотр в VS Code: расширение **Markdown Preview Mermaid Support** (`bierner.markdown-mermaid`), затем `Ctrl+Shift+V`.

---

## 1. Жизненный цикл одного листинга (верхний уровень)

```mermaid
stateDiagram-v2
    [*] --> WAIT_LISTING
    WAIT_LISTING --> BOOK_SYNC : монета появилась, зафиксирован listingStartTimeMs ⚠В13
    note right of BOOK_SYNC
        Параллельно и независимо стартуют
        price_timeline 60s / 1s и micro_timeline 10s / 100ms
        (диаграмма 8)
    end note
    BOOK_SYNC --> BOOK_SYNC : sequence gap, resync
    BOOK_SYNC --> WARMUP : snapshot + непрерывные diff
    WARMUP --> EVALUATE : прошло bookWarmupMs
    EVALUATE --> EVALUATE : фильтр не пройден, окно открыто ⚠В1
    EVALUATE --> SKIPPED : SKIP_* или истёк tradeWindowSec
    EVALUATE --> PRE_SEND : ENTER_ALLOWED
    PRE_SEND --> SKIPPED : стакан устарел или цена ушла
    PRE_SEND --> BUY_PENDING : LIMIT BUY отправлен
    BUY_PENDING --> NO_FILL : buyFillTimeoutMs и executedQty = 0
    BUY_PENDING --> POSITION_OPEN : полный fill или partial + cancel остатка
    POSITION_OPEN --> CLOSED : TP1 и TP2 исполнены
    POSITION_OPEN --> URGENT_EXIT : TIME_STOP, BE, FAST_DROP, MAX_HOLD
    POSITION_OPEN --> CRITICAL_EXIT : DEPTH_DISAPPEARED, FAST_WINDOW_DUMP, HARD_STOP
    URGENT_EXIT --> CLOSED : остаток = 0
    URGENT_EXIT --> CRITICAL_EXIT : не исполнен после maxExitReprices
    CRITICAL_EXIT --> CLOSED : State = COMPLETED
    CRITICAL_EXIT --> STUCK : 3 полные попытки неудачны ⚠В10
    SKIPPED --> [*]
    NO_FILL --> [*]
    CLOSED --> [*]
    STUCK --> [*]
```

---

## 2. Локальный стакан и его целостность (работает всё время)

```mermaid
flowchart TD
    S(["Старт символа"]) --> WS["Подписка WS: diff-depth + trades"]
    WS --> BUF["Буферизовать diff-события"]
    BUF --> SNAP["REST snapshot стакана"]
    SNAP --> APPLY["Отбросить устаревшие diff,<br/>остальные применять по порядку версий"]
    APPLY --> CHK{"fromVersion == prevToVersion + 1 ?"}
    CHK -- да --> VALID["bookIntegrityValid = true<br/>lastBookUpdateMs = now<br/>DepthSample → ring buffer 1000ms"]
    VALID --> NEXT["Следующее diff-событие"]
    NEXT --> CHK
    CHK -- нет --> GAP["BOOK_SEQUENCE_GAP<br/>bookIntegrityValid = false<br/>очистить persistence-буферы"]
    GAP --> RS["BOOK_RESYNC_STARTED"]
    RS --> BUF
    VALID -. "первый валидный стакан" .-> FIRST["Один раз зафиксировать<br/>firstValidMidPrice, firstValidAsk, firstValidExitVWAP"]
    GAP -.-> Q8["⚠В8 Что делать с ОТКРЫТОЙ позицией,<br/>пока стакан невалиден?"]

    classDef warn fill:#fde2e2,stroke:#c0392b,color:#000
    class Q8 warn
```

---

## 3. Фильтры входа (EVALUATE)

```mermaid
flowchart TD
    A(["Прошло bookWarmupMs"]) --> W{"now ≤ listingStart + tradeWindowSec 8s?"}
    W -- нет --> X0["SKIP_ENTRY_WINDOW_EXPIRED"]
    W -- да --> E{"entriesCount #lt; maxEntriesPerSymbol 1?"}
    E -- нет --> X1["SKIP_MAX_ENTRIES"]
    E -- да --> I{"bookIntegrityValid?"}
    I -- нет --> R
    I -- да --> B{"bestBid #gt; 0 и bestAsk #gt; bestBid?"}
    B -- нет --> XB["SKIP_NO_BID / SKIP_BAD_BOOK"]
    B -- да --> SP{"spreadPct ≤ maxSpreadPct 1.2?"}
    SP -- нет --> XS["SKIP_SPREAD_TOO_WIDE"]
    SP -- да --> D7{"depthBid07 ≥ orderUSDT × 15? ⚠В12"}
    D7 -- нет --> XD7["SKIP_DEPTH_BID_07_TOO_LOW"]
    D7 -- да --> D15{"depthBid15 ≥ orderUSDT × 30?"}
    D15 -- нет --> XD15["SKIP_DEPTH_BID_15_TOO_LOW"]
    D15 -- да --> DA{"depthAsk07 ≥ orderUSDT × 8?"}
    DA -- нет --> XDA["SKIP_DEPTH_ASK_07_TOO_LOW"]
    DA -- да --> VW{"exitVWAP для планового qty посчитан? ⚠В14"}
    VW -- нет --> XVW["SKIP_NO_EXIT_LIQUIDITY"]
    VW -- да --> SL{"estimatedSlippagePct ≤ 1.5?"}
    SL -- нет --> XSL["SKIP_EXIT_SLIPPAGE_TOO_HIGH"]
    SL -- да --> ACT{"updatesPerSec ≥ 6 и bestUpdatesPerSec ≥ 3? ⚠В1"}
    ACT -- нет --> XACT["SKIP_BOOK_NOT_ACTIVE"]
    ACT -- да --> AGE{"bookAgeMs ≤ 500?"}
    AGE -- нет --> XAGE["SKIP_STALE_BOOK"]
    AGE -- да --> TK{"tickPct #lt; 0.3?"}
    TK -- нет --> XTK["SKIP_TICK_TOO_LARGE"]
    TK -- да --> COL["COLLECT_ONLY: посчитать и залогировать<br/>directional, chase, depth persistence, depth removal<br/>(на решение пока НЕ влияют)"]
    COL --> OK(["ENTER_ALLOWED → диаграмма 4"])

    XB & XS & XD7 & XD15 & XDA & XVW & XSL & XACT & XAGE & XTK --> R{"⚠В1 SKIP окончательный<br/>или повтор на следующем тике?"}
    R -. "повтор через recalcIntervalMs" .-> W
    R -- окончательный --> END(["SKIPPED"])
    X0 --> END
    X1 --> END

    classDef warn fill:#fde2e2,stroke:#c0392b,color:#000
    class R warn
```

---

## 4. Отправка LIMIT BUY и обработка fill

```mermaid
flowchart TD
    A(["ENTER_ALLOWED"]) --> F{"now − lastBookUpdateMs ≤ maxOrderSendBookAgeMs 500?<br/>и bookIntegrityValid"}
    F -- нет --> X1["SKIP_STALE_BOOK_BEFORE_SEND"]
    F -- да --> R["Обновить bestAsk"]
    R --> C{"Свежий bestAsk в пределах cap? ⚠В2"}
    C -- нет --> X2["SKIP_PRICE_MOVED_BEFORE_SEND"]
    C -- да --> P["buyLimitPrice = roundDown(bestAsk × 1.01, priceStep)<br/>qty = floor(orderUSDT / buyLimitPrice, qtyStep)"]
    P --> N{"qty × buyLimitPrice ≤ orderUSDT<br/>и ≥ minNotional?"}
    N -- нет --> X3["Вход запрещён"]
    N -- да --> SEND["Отправить LIMIT BUY<br/>entriesCount++"]
    SEND --> WAIT{"Что за buyFillTimeoutMs 1200ms?"}
    WAIT -- "полный fill" --> POS
    WAIT -- "fill нет" --> CAN["Cancel BUY, дождаться ack"]
    CAN --> CHK{"После ack: executedQty #gt; 0? ⚠В9"}
    CHK -- нет --> X4["BUY_NOT_FILLED_FAST"]
    CHK -- да --> SM
    WAIT -- "частичный fill" --> PC["Cancel остатка, дождаться ack"]
    PC --> SM{"executedNotional ≥ minManagePositionUSDT 1.0?"}
    SM -- нет --> X5["BUY_PARTIAL_TOO_SMALL<br/>⚠В5 монеты уже куплены — что с ними?"]
    SM -- да --> POS["avgBuyPrice по fill-ам<br/>positionQty = фактический баланс с учётом комиссии ⚠В6<br/>ENTER_FILLED, entryFillTimeMs"]
    POS --> TP["TP1: LIMIT SELL 50% по avgBuyPrice × 1.02<br/>TP2: LIMIT SELL остаток по avgBuyPrice × 1.04<br/>⚠В5 каждая часть ≥ minNotional?"]
    TP --> M(["POSITION_OPEN → диаграмма 5"])

    classDef warn fill:#fde2e2,stroke:#c0392b,color:#000
    class X5 warn
```

---

## 5. Сопровождение позиции (каждые recalcIntervalMs)

Порядок проверок в ТЗ не задан — ниже **предлагаемый** приоритет: сначала аварийные триггеры, затем защитные, затем временные.

```mermaid
flowchart TD
    T(["Тик recalcIntervalMs 100ms"]) --> U["Обновить bestBid, spread, depth<br/>realExitPrice = exitVWAP(remainingQty)<br/>netPnL = gross − estBuyFee − estSellFee<br/>MFENet, MAENet, elapsedSec от fill"]
    U --> TPF{"TP исполнены?"}
    TPF -- "остаток = 0" --> CL(["TP_FILLED → CLOSED"])
    TPF -- "нет / частично,<br/>remainingQty пересчитан" --> V{"exitVWAP посчитан?"}
    V -- нет --> DD["DEPTH_DISAPPEARED"]
    V -- да --> HS{"netPnL ≤ hardStopLossPct −8%?"}
    HS -- да --> HSX["HARD_STOP"]
    HS -- нет --> FW{"shortWindowReturnPct за 1s ≤ −6%?<br/>⚠В15 в каком окне действует?"}
    FW -- да --> FWX["FAST_WINDOW_DUMP"]
    FW -- нет --> FD{"elapsed ≤ 3s и netPnL ≤ −2.5%?"}
    FD -- да --> FDX["FAST_DROP_STOP"]
    FD -- нет --> BE{"BE включён (MFENet ≥ 1.5%)<br/>и netPnL ≤ beFloorNetPct 0.15%?"}
    BE -- да --> BEX["BREAK_EVEN_STOP"]
    BE -- нет --> TN{"elapsed ≥ 3s и netPnL #lt; −0.3%? ⚠В3"}
    TN -- да --> TNX["TIME_STOP_NEGATIVE"]
    TN -- нет --> NE{"elapsed ≥ 5s и netPnL #lt;<br/>0.3 + currentSpread × 1.0?"}
    NE -- да --> NEX["TIME_STOP_NO_EDGE"]
    NE -- нет --> NM{"elapsed ≥ 8s и нет импульса?<br/>⚠В4 импульс не определён"}
    NM -- да --> NMX["TIME_STOP_NO_MOMENTUM"]
    NM -- нет --> MH{"elapsed ≥ maxHoldSec 15s?"}
    MH -- да --> MHX["MAX_HOLD_TIME"]
    MH -- нет --> T

    DD & HSX & FWX --> CRIT(["→ Критический выход, диаграмма 7"])
    FDX & BEX & TNX & NEX & NMX & MHX --> URG(["→ Срочный выход, диаграмма 6"])

    classDef crit fill:#fde2e2,stroke:#c0392b,color:#000
    classDef urg fill:#fff3cd,stroke:#b8860b,color:#000
    class DD,HSX,FWX crit
    class FDX,BEX,TNX,NEX,NMX,MHX urg
```

---

## 6. Срочный выход: aggressive LIMIT SELL с перестановками

```mermaid
flowchart TD
    S(["Триггер срочного выхода, reasonCode"]) --> C["Отменить активные TP, дождаться ack<br/>перечитать исполненный объём TP ⚠В9"]
    C --> R{"remainingQty #gt; 0?"}
    R -- нет --> DONE(["CLOSED — TP успел исполниться"])
    R -- да --> N0["n = 0"]
    N0 --> DUST{"remainingQty × bestBid ≥ minNotional? ⚠В5"}
    DUST -- нет --> DX["Остаток-пыль: биржа не примет SELL"]
    DUST -- да --> L["sellLimitPrice = roundDown(bestBid × 0.995, priceStep)<br/>LIMIT SELL remainingQty"]
    L --> W{"Исполнен за exitOrderTimeoutMs 400ms?"}
    W -- полностью --> DONE2(["CLOSED"])
    W -- "нет / частично" --> CN["Cancel, дождаться ack<br/>remainingQty = фактический остаток"]
    CN --> N{"n #lt; maxExitReprices 3?"}
    N -- да --> INC["n++, обновить bestBid"]
    INC --> DUST
    N -- нет --> CRIT(["urgentLimitExitNotFilledAfterMaxReprices<br/>→ Критический выход, диаграмма 7"])
    W -. "во время ожидания: DEPTH_DISAPPEARED,<br/>FAST_WINDOW_DUMP, HARD_STOP" .-> CRIT

    classDef warn fill:#fde2e2,stroke:#c0392b,color:#000
    class DX warn
```

---

## 7. Критический выход: RequestSellOrderMinPrice (имитация MARKET SELL через веб-форму)

```mermaid
flowchart TD
    S(["criticalFallback"]) --> LOG["Сохранить reasonCode, bestBidBefore, expectedExitVWAP<br/>(VWAP — только диагностика, не запрет)"]
    LOG --> A["Попытка k из criticalFullAttempts 3"]
    A --> D["Пауза criticalRetryDelaysSec[k]: 0 / 1 / 2 s"]
    D --> F1["Открыть вкладку SELL, очистить sellPrice"]
    F1 --> Q{"Quantity = 99999999 принят?<br/>до 20 попыток × 100ms"}
    Q -- нет --> RL
    Q -- да --> TT{"Total = 1.2 принят?<br/>до 20 попыток × 100ms"}
    TT -- нет --> RL
    TT -- да --> CLICK["Нажать Sell, перехватить ответ создания ордера"]
    CLICK --> RESP{"code = 200 и msg = success<br/>за criticalCreateResponseTimeoutMs 10s?"}
    RESP -- нет --> RL["LoadPage — перезагрузка страницы"]
    RL --> MORE{"Попытки остались?"}
    MORE -- да --> A
    MORE -- нет --> FAIL["CRITICAL_LIMIT_SELL_FAILED<br/>стоп торговли по монете<br/>⚠В10 позиция остаётся открытой"]
    RESP -- да --> WAIT["orderId; пауза criticalOrderStatusDelayMs 500ms<br/>GetOrder(orderId)"]
    WAIT --> ST{"order.State?"}
    ST -- COMPLETED --> OK(["CLOSED: fills, actualAvgSellPrice, executionGapPct"])
    ST -- "частично / не исполнен" --> PR["CRITICAL_LIMIT_SELL_PARTIAL<br/>remainingQty = остаток ⚠В7"]
    PR --> MORE

    classDef warn fill:#fde2e2,stroke:#c0392b,color:#000
    class FAIL,PR warn
```

---

## 8. Независимое логирование (не останавливается при SKIP / CLOSED / critical)

```mermaid
flowchart LR
    L(["listingStartTimeMs"]) --> P["price_timeline<br/>каждые priceLogIntervalSec 1s<br/>в течение priceLogDurationSec 60s → 60 строк"]
    L --> M["micro_timeline<br/>каждые microLogIntervalMs 100ms<br/>в течение microLogDurationSec 10s → 100 строк"]
    P --> ROW["Строка создаётся всегда:<br/>последнее валидное состояние,<br/>sourceAgeMs, isStale, dataAvailable"]
    M --> ROW
    P --> SUM["price_timeline_summary после окна:<br/>max/min, maxRisePct, maxDrawdownPct,<br/>missing/stale samples, enteredTrade"]
    EV["События: trade_decisions, order_events,<br/>position_events, book_snapshots,<br/>critical_sell_metrics, latency chain"] -.-> DB[("Хранилище логов<br/>+ configVersion")]
    ROW --> DB
    SUM --> DB
```

---

## Найденные логические проблемы и вопросы к клиенту

> Ответы клиента (12.10.2026) и принятые решения — в [resheniya_po_voprosam.md](resheniya_po_voprosam.md).

### Критичные (влияют на деньги или делают логику неработоспособной)

**⚠В1. Фильтры входа проверяются один раз или повторно?** ТЗ не говорит, что происходит после SKIP внутри `tradeWindowSec`. Если проверка однократная, фильтр активности почти всегда даст отказ: `bookWarmupMs = 500`, а `updatesPerSec` считается за окно 1000 ms. При 100-ms потоке diff-depth за 500 ms приходит не больше 5 обновлений при пороге 6, то есть `SKIP_BOOK_NOT_ACTIVE` срабатывает всегда.
→ Предложение: повторять фильтры на каждом тике до конца `tradeWindowSec`; окончательный SKIP давать только для «жёстких» причин (`MAX_ENTRIES`, `WINDOW_EXPIRED`). Либо поднять warmup до 1000 ms или больше.

**⚠В2. `SKIP_PRICE_MOVED_BEFORE_SEND` никогда не сработает.** По ТЗ `buyLimitPrice` пересчитывается от *свежего* bestAsk, поэтому «bestAsk выше cap» невозможен: cap просто сдвигается вместе с ценой. Это и есть погоня за ценой, от которой ТЗ хочет защититься.
→ Предложение: брать за якорь `decisionAsk` (ask, на котором прошли фильтры). Если `freshAsk > decisionAsk × (1 + cap)`, то SKIP; в остальных случаях `buyLimitPrice` считать от `decisionAsk`.

**⚠В3. Time-stop срабатывает «в момент» или «начиная с момента»?** «Каждое действие должно срабатывать один раз» можно понять так: проверить ровно в t = 3 s. Тогда после 5 s (и до 15 s) позицию защищают только BE (если включён) и hard stop −8 %.
→ Предложение: условие `elapsed ≥ X` проверяется непрерывно, «один раз» относится только к самому выходу.
Отдельно: сразу после входа `netPnL ≈ −(spread + fees + проскальзывание входа) ≈ −0.7…−1.7 %`. При пороге −0.3 % `TIME_STOP_NEGATIVE` закроет почти каждую сделку, если цена за 3 s не вырастет примерно на 1 %. Уточнить у клиента, задумано ли так.

**⚠В4. «Импульс» для `TIME_STOP_NO_MOMENTUM` не формализован.** В ТЗ сказано только «рост цены, активный стакан, сохранение depthBid, исполнение TP». Нужна формула и пороги в конфиге.

**⚠В5. minNotional и мелкие остатки.** Частичный fill, деление на TP1/TP2 (по 50 %) и остатки после частичных SELL могут оказаться меньше минимального объёма ордера на бирже. Такой SELL биржа отклонит, и позиция «застрянет». Для `BUY_PARTIAL_TOO_SMALL` логика не описана, хотя монеты уже куплены.
→ Предложение: если позиция меньше 2 × minNotional, не делить её на TP1/TP2 и ставить один TP. Остаток меньше minNotional помечать как DUST и не пытаться продать.

**⚠В6. Комиссия BUY списывается в купленной монете.** Если не включена оплата комиссии в MX, фактический баланс меньше `executedQty`. Если выставить TP или SELL на `executedQty`, биржа ответит «insufficient balance».
→ `positionQty` брать из фактического баланса или из fills с учётом `commission` / `commissionAsset`.

**⚠В7. RequestSellOrderMinPrice на маленьком остатке не работает.** Форма ставит цену ≈ `1.2 / qty`. Если остаток по рынку стоит меньше 1.2 USDT, эта цена окажется *выше* рынка, и ордер не исполнится. Значит, «повторять критическую продажу для остатка» (требование v2.1) для мелких остатков не сработает. Кроме того, нужно проверить на реальной бирже, не отклоняет ли MEXC лимитные цены, слишком далёкие от рынка.

**⚠В8. Открытая позиция при невалидном стакане (gap / resync).** В это время `exitVWAP` и PnL не считаются, и ТЗ не говорит, что делать.
→ Предложение: на время resync заморозить стопы не дольше N ms (параметр в конфиге). Если стакан не восстановился за это время, запускать срочный выход по bestBid из REST.

**⚠В9. Гонки при отмене ордеров.** Fill может прийти между решением и отменой: у BUY по таймауту, у TP перед срочным выходом, у срочного SELL перед перестановкой. После каждой отмены нужно дождаться ack и перечитать `executedQty`. Иначе бот попытается продать больше, чем у него есть, или потеряет часть позиции из учёта.

**⚠В10. `CRITICAL_LIMIT_SELL_FAILED` оставляет позицию открытой.** По ТЗ бот после этого просто «останавливает торговлю по монете». Нужны алерт (Telegram) и либо ручное вмешательство, либо периодический повтор.

**⚠В11. Гибрид API и веб-формы (главный архитектурный вопрос).** `RequestSellOrderMinPrice` — это автоматизация браузера (в исходном проекте C#: `MxcWeb.cs`). Python-боту понадобится параллельно с API держать браузер с залогиненной сессией MEXC. Нужно уточнить у клиента: **доступна ли торговля новым листингом через API в первые секунды?** На MEXC для части новых пар API-торговля на старте бывает закрыта. Если закрыта, через веб придётся делать и вход, и все выходы, а это меняет всю архитектуру и задержки.

### Существенные

**⚠В12. Зоны depthBid считаются от midPrice, а продажа идёт по bestBid.** При spread 1.2 % bestBid = mid − 0.6 %, и в зону «−0.7 % от mid» попадает только 0.1 % ниже bestBid. При spread от 1.4 % bestBid оказывается ниже `lowerBid07`, и `depthBid07 = 0`. Получается скрытый, более жёсткий spread-фильтр.
→ Предложение: зоны bids считать от bestBid, зоны asks — от bestAsk.

**⚠В13. Не описан источник листингов.** Откуда бот узнаёт символ и `listingStartTimeMs`: анонсы MEXC, exchangeInfo, календарь? Также не заданы максимум одновременных листингов, распределение общего баланса и дневной лимит (`dailyLossLimitUSDT = null`).

**⚠В14. Порядок шагов.** Фильтр exitVWAP (шаг 7) требует плановый `qty`, а он вычисляется только на шаге 12. Для фильтра использовать оценку `qty ≈ orderUSDT / bestAsk`.

### Мелкие и неоднозначности

**⚠В15.**
- `hardMaxSpreadPct = 1.5` есть в конфиге, но в логике не используется.
- Окно действия `FAST_WINDOW_DUMP` не задано: только первые `fastDropWindowSec` или всё время удержания.
- Направление округления `roundToValidPrice` для TP не задано. Для TP логично округлять вверх, для стопов вниз.
- Приоритет при одновременном срабатывании нескольких триггеров не задан. На диаграмме 5 предложен порядок.

**⚠В16. Время выхода.** Срочный выход занимает примерно 4 × 400 ms плюс задержки cancel/ack, то есть около 2–3 s. Критический выход в худшем случае: 3 × 10 s + 3 s ≈ 33 s. При резком сливе на листинге это очень долго. Это не ошибка, но клиент должен понимать эти цифры.
