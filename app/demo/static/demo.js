"use strict";

// Demo page. It has no payment logic of its own. Payments go through the public API
// (POST/GET /api/v1/payments); the outbox events of a payment, the queues and the demo
// webhook receiver are read through the demo helpers (/demo/api/*). Data from the API is
// always inserted as text, never as HTML.

const AMOUNT = "1500.00";
const CURRENCY = "RUB";
// The consumer sends webhooks from inside docker compose, hence the service name.
const WEBHOOK_URL = "http://webhook-receiver:9000/webhook";
const KEY_STORAGE = "payment-processing-demo:api-key";
const DEFAULT_KEY = "local-dev-api-key"; // the dev-only API_KEY from docker-compose.yml
const WATCH_MAX_SECONDS = 90;
const SIDE_REFRESH_MS = 2000;
const BREAKER_OUTAGE_SECONDS = 12;
const BREAKER_PAYMENTS = 8;

const SCENARIOS = [
  {
    kind: "single",
    title: "Обычный платёж",
    what: "Один платёж на 1 500,00 RUB.",
    expect:
      "API сразу отвечает 202 и статус pending. Relay публикует событие из outbox, consumer " +
      "через 2-5 с получает ответ шлюза (в 90% случаев одобрение) и шлёт вебхук клиенту.",
  },
  {
    kind: "series",
    title: "Десять платежей",
    what: "10 платежей подряд.",
    expect:
      "Примерно 9 платежей будут оплачены и примерно 1 получит отказ шлюза: это «10% " +
      "ошибка» из задания. Отказ это обычный исход: статус failed и вебхук, без повторов. " +
      "Вебхук получает каждый платёж.",
  },
  {
    kind: "idempotency",
    title: "Повтор запроса",
    what: "Тот же запрос ещё раз, пять одновременных с одним ключом, тот же ключ с другим телом.",
    expect:
      "Повторы и одновременные запросы возвращают тот же платёж, событие в outbox одно. " +
      "Тот же ключ с другим телом: 422.",
  },
  {
    kind: "validation",
    title: "Ошибки клиента",
    what: "Запросы без ключа, без Idempotency-Key, с суммой-дробью числом, с чужой валютой.",
    expect: "Каждый отклоняется с понятным кодом, платёж не создаётся.",
  },
  {
    kind: "dlq",
    title: "Сервер клиента отвечает 500",
    what: "Приёмник вебхуков «падает», создаётся платёж.",
    expect:
      "Платёж проводится, а вебхук не принимается: три попытки с паузами 2 и 4 с, потом " +
      "сообщение уходит в DLQ. Сервер чинится, сообщение возвращается из DLQ, вебхук " +
      "доставлен, повторного списания нет.",
  },
  {
    kind: "breaker",
    title: "Сервер клиента долго лежит",
    what: `Приёмник лежит ${BREAKER_OUTAGE_SECONDS} с, за это время ${BREAKER_PAYMENTS} платежей.`,
    expect:
      "После 5 неудач подряд circuit breaker размыкает цепь: остальные вебхуки ждут в " +
      "очереди задержки без запросов и без траты попыток. Сервер оживает, пробный запрос " +
      "проходит, все вебхуки доставлены, DLQ не растёт.",
  },
];

const PAYMENT_LABELS = {
  pending: ["в обработке", "info"],
  succeeded: ["оплачен", "ok"],
  failed: ["отказ шлюза", "bad"],
};

const activeRuns = new Set();
let activeButton = null;
let keyRejected = false;
let receiverFailing = false;

// ---------------------------------------------------------------- helpers

const $ = (id) => document.getElementById(id);

function h(tag, props, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props || {})) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") node.className = value;
    else if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2), value);
    } else node.setAttribute(key, value === true ? "" : String(value));
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

const toneClass = (tone) => (tone ? `tone-${tone}` : "");
const fmtSeconds = (s, digits = 1) => s.toFixed(digits).replace(".", ",");
const fmtMoney = (amount, currency) => `${String(amount).replace(".", ",")} ${currency}`;
const shortId = (id) => (id ? `${id.slice(0, 8)}…` : "");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
// The API sends microseconds; not every browser parses more than three fractional digits.
const parseTime = (iso) => Date.parse(iso.replace(/(\.\d{3})\d+/, "$1"));
const secondsBetween = (fromIso, toIso) => (parseTime(toIso) - parseTime(fromIso)) / 1000;
const clock = (iso) => new Date(parseTime(iso)).toLocaleTimeString("ru-RU");
const paymentLabel = (status) => PAYMENT_LABELS[status] || [status, ""];

function randomKey() {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  return "demo-" + Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

// ---------------------------------------------------------------- API

const apiKey = () => $("api-key").value.trim();

// Response headers worth showing in the "API requests" block of a card.
const SHOWN_RESPONSE_HEADERS = ["location", "idempotent-replayed", "www-authenticate", "retry-after"];

// run: the card whose "API requests" block records this exchange (null: the side panels).
// key: null sends no X-API-Key at all (the "client errors" scenario).
async function api(method, path, { run = null, body, headers = {}, key } = {}) {
  const sendKey = key === undefined ? apiKey() : key;
  const init = { method, headers: { Accept: "application/json", ...headers } };
  if (sendKey !== null) init.headers["X-API-Key"] = sendKey;
  if (body !== undefined) {
    init.headers["Content-Type"] = "application/json";
    init.body = typeof body === "string" ? body : JSON.stringify(body);
  }
  const started = performance.now();
  const response = await fetch(path, init);
  const text = await response.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = null;
  }
  if (sendKey !== null) setKeyRejected(response.status === 401);
  if (run) {
    run.http.record({
      method,
      path,
      requestHeaders: init.headers,
      requestBody: init.body,
      status: response.status,
      responseHeaders: SHOWN_RESPONSE_HEADERS.filter((name) => response.headers.has(name)).map(
        (name) => [name, response.headers.get(name)],
      ),
      responseBody: data !== null ? data : text,
      ms: performance.now() - started,
    });
  }
  return { status: response.status, headers: response.headers, data };
}

function errorText(result) {
  const detail = result.data && result.data.detail;
  if (Array.isArray(detail) && detail.length) {
    const first = detail[0];
    return `${(first.loc || []).slice(1).join(".")}: ${first.msg}`;
  }
  if (detail && typeof detail === "object" && detail.code) return detail.code;
  if (detail) return JSON.stringify(detail);
  return `HTTP ${result.status}`;
}

function paymentBody(overrides = {}) {
  return {
    amount: AMOUNT,
    currency: CURRENCY,
    description: "Демо-платёж",
    metadata: { source: "demo-page" },
    webhook_url: WEBHOOK_URL,
    ...overrides,
  };
}

// The first argument of every helper is the card that records the request (or null).
const createPayment = (run, key = randomKey(), overrides = {}) =>
  api("POST", "/api/v1/payments", {
    run,
    body: paymentBody(overrides),
    headers: { "Idempotency-Key": key },
  });
const getPayment = (run, id) => api("GET", `/api/v1/payments/${encodeURIComponent(id)}`, { run });
const getOutbox = (run, id) =>
  api("GET", `/demo/api/payments/${encodeURIComponent(id)}/outbox`, { run });
const getHooks = (run, id) =>
  api("GET", `/demo/api/receiver/received${id ? `?payment_id=${encodeURIComponent(id)}` : ""}`, { run });
const getQueues = (run) => api("GET", "/demo/api/queues", { run });
const setReceiverFail = (run, fail) =>
  api("PUT", "/demo/api/receiver/mode", { run, body: { fail } });
const replayDlq = (run) => api("POST", "/demo/api/dlq/replay", { run });

async function dlqCount(run) {
  const result = await getQueues(run);
  if (result.status !== 200 || !Array.isArray(result.data)) return null;
  const dlq = result.data.find((q) => q.role === "dlq");
  return dlq ? dlq.messages : null;
}

async function postponedCount(run) {
  const result = await getQueues(run);
  if (result.status !== 200 || !Array.isArray(result.data)) return null;
  return result.data.filter((q) => q.role === "postpone").reduce((n, q) => n + q.messages, 0);
}

// Everything the page knows about one payment: the API's view, its outbox events and what
// the client's server received (oldest first).
async function snapshot(run, id) {
  const [payment, outbox, hooks] = await Promise.all([
    getPayment(run, id),
    getOutbox(run, id),
    getHooks(run, id),
  ]);
  if (payment.status !== 200) throw new Error(`GET /api/v1/payments/{id}: HTTP ${payment.status}`);
  return {
    payment: payment.data,
    outbox: outbox.status === 200 && Array.isArray(outbox.data) ? outbox.data : [],
    hooks: hooks.status === 200 && Array.isArray(hooks.data) ? [...hooks.data].reverse() : [],
  };
}

// ---------------------------------------------------------------- "API requests" block

const STATUS_TEXT = {
  200: "OK",
  202: "Accepted",
  400: "Bad Request",
  401: "Unauthorized",
  404: "Not Found",
  422: "Unprocessable Content",
  500: "Internal Server Error",
  502: "Bad Gateway",
};

function prettyBody(body) {
  if (body === undefined || body === null || body === "") return null;
  if (typeof body !== "string") return JSON.stringify(body, null, 2);
  try {
    return JSON.stringify(JSON.parse(body), null, 2);
  } catch {
    return body;
  }
}

// One request and its response, written the way they go over the wire.
function renderExchange(x, count) {
  const requestLines = [`${x.method} ${x.path} HTTP/1.1`];
  for (const [name, value] of Object.entries(x.requestHeaders)) {
    if (name === "Accept") continue;
    requestLines.push(`${name}: ${name === "X-API-Key" ? "••••••••  (скрыт)" : value}`);
  }
  const requestBody = prettyBody(x.requestBody);
  const request = requestBody ? `${requestLines.join("\n")}\n\n${requestBody}` : requestLines.join("\n");

  const responseLines = [`HTTP/1.1 ${x.status} ${STATUS_TEXT[x.status] || ""}`.trimEnd()];
  for (const [name, value] of x.responseHeaders) responseLines.push(`${name}: ${value}`);
  const responseBody = prettyBody(x.responseBody);
  const response = responseBody ? `${responseLines.join("\n")}\n\n${responseBody}` : responseLines.join("\n");

  const tone = x.status < 300 ? "ok" : x.status < 500 ? "warn" : "bad";
  // Grouped view: how many times this address was polled. Full view: order and time.
  const meta = count
    ? `${Math.round(x.ms)} мс${count > 1 ? " · показан последний ответ" : ""}`
    : `+${fmtSeconds(x.at)} с · ${Math.round(x.ms)} мс`;
  return h(
    "details",
    { class: "exchange" },
    h(
      "summary",
      null,
      count ? null : h("span", { class: "exchange-no" }, `${x.index}.`),
      h("span", { class: `badge ${toneClass(tone)}` }, String(x.status)),
      count > 1 ? h("span", { class: "badge tone-info", title: `Запрошено ${count} раз` }, `×${count}`) : null,
      h("code", null, `${x.method} ${x.path}`),
      h("span", { class: "exchange-meta" }, meta),
    ),
    h("div", { class: "exchange-label" }, "Запрос"),
    h("pre", null, request),
    h("div", { class: "exchange-label" }, "Ответ"),
    h("pre", null, response),
  );
}

// "1 строка, 2 строки, 5 строк".
function plural(n, [one, few, many]) {
  const mod10 = n % 10;
  const mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return one;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return few;
  return many;
}

// Everything a card sent to the API. By default GETs of the same address (the page polls
// every second) collapse into one row with the latest answer and a ×N counter; a button
// switches to every request in order.
class HttpLog {
  constructor(run) {
    this.run = run;
    this.exchanges = [];
    this.showAll = false;
    this.polls = new Map();
    this.title = h("span");
    this.toggle = h("button", {
      type: "button",
      class: "link-button",
      onclick: () => this.setShowAll(!this.showAll),
    });
    this.list = h("div", { class: "exchanges" });
    this.el = h(
      "details",
      { class: "http" },
      h("summary", null, this.title),
      h(
        "p",
        { class: "hint" },
        "Что страница отправила и что получила, в виде HTTP. Ключ скрыт. Страница опрашивает " +
          "платёж раз в секунду, поэтому одинаковые GET свёрнуты в одну строку с последним " +
          "ответом и счётчиком ×N.",
      ),
      this.toggle,
      this.list,
    );
    this.updateHeader();
  }

  record(x) {
    x.index = this.exchanges.length + 1;
    x.at = this.run.elapsed();
    this.exchanges.push(x);
    if (this.showAll) this.list.append(renderExchange(x, 0));
    else this.addGrouped(x);
    this.updateHeader();
  }

  addGrouped(x) {
    const pollKey = x.method === "GET" ? x.path : null;
    const previous = pollKey ? this.polls.get(pollKey) : null;
    const count = previous ? previous.count + 1 : 1;
    const node = renderExchange(x, count);
    if (previous) {
      node.open = previous.node.open; // keep it expanded if the reader opened it
      previous.node.replaceWith(node);
    } else {
      this.list.append(node);
    }
    if (pollKey) this.polls.set(pollKey, { node, count });
  }

  setShowAll(showAll) {
    this.showAll = showAll;
    this.polls.clear();
    this.list.replaceChildren();
    for (const x of this.exchanges) {
      if (showAll) this.list.append(renderExchange(x, 0));
      else this.addGrouped(x);
    }
    this.updateHeader();
  }

  updateHeader() {
    const total = this.exchanges.length;
    const rows = this.list.children.length;
    const requests = `${total} ${plural(total, ["запрос", "запроса", "запросов"])}`;
    this.title.textContent =
      !this.showAll && rows < total
        ? `Запросы к API: ${requests}, в списке ${rows} ${plural(rows, ["строка", "строки", "строк"])}`
        : `Запросы к API: ${requests}`;
    this.toggle.textContent = this.showAll
      ? "Свернуть одинаковые опросы"
      : `Показать все ${total} по порядку`;
    this.toggle.hidden = !this.showAll && rows === total;
  }
}

// ---------------------------------------------------------------- run card

class Run {
  constructor(scenario) {
    this.t0 = performance.now();
    this.timer = null;
    this.alive = true; // false once the card is removed: no more requests from this run
    activeRuns.add(this);
    this.badge = h("span", { class: "badge tone-info" }, "отправляем");
    this.statusLine = h("div", { class: "status-line" });
    this.summary = h("div", { class: "summary" });
    this.extra = h("div");
    this.body = h("div");
    this.logList = h("ol");
    this.http = new HttpLog(this);
    this.foot = h("div", { class: "run-foot" });
    this.el = h(
      "article",
      { class: "run" },
      h("div", { class: "run-head" }, h("h3", null, scenario.title), this.badge),
      h("p", { class: "expect" }, h("b", null, "Ожидаем. "), scenario.expect),
      this.statusLine,
      this.summary,
      this.extra,
      this.body,
      h("details", { class: "log", open: true }, h("summary", null, "Журнал"), this.logList),
      this.http.el,
      this.foot,
    );
    $("runs").prepend(this.el);
    $("empty").hidden = true;
    if (window.matchMedia("(max-width: 900px)").matches) {
      this.el.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  elapsed() {
    return (performance.now() - this.t0) / 1000;
  }

  log(text, tone) {
    const time = h("span", { class: "t" }, `+${fmtSeconds(this.elapsed())} с`);
    this.logList.append(h("li", { class: tone || null }, time, text));
  }

  setBadge(text, tone) {
    this.badge.textContent = text;
    this.badge.className = `badge ${toneClass(tone)}`;
  }

  busy(text) {
    this.idle();
    const update = () =>
      this.statusLine.replaceChildren(
        h("span", { class: "spinner", "aria-hidden": "true" }),
        h("span", null, text()),
      );
    update();
    this.timer = setInterval(update, 250);
  }

  idle() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.statusLine.replaceChildren();
  }

  fail(text) {
    this.idle();
    this.setBadge("ошибка", "bad");
    this.log(text, "bad");
  }

  stop() {
    this.alive = false;
    this.idle();
    activeRuns.delete(this);
  }

  done() {
    activeRuns.delete(this);
  }
}

function checkList(run) {
  const list = h("ul", { class: "checks" });
  run.extra.replaceChildren(list);
  let allOk = true;
  return {
    add(ok, text) {
      list.append(h("li", null, h("span", { class: ok ? "mark-ok" : "mark-bad" }, ok ? "✓ " : "✗ "), text));
      allOk = allOk && ok;
      return ok;
    },
    get ok() {
      return allOk;
    },
  };
}

// ---------------------------------------------------------------- one payment, step by step

function steps(s) {
  const p = s.payment;
  const at = (iso) => (iso ? `+${fmtSeconds(secondsBetween(p.created_at, iso), 2)} с` : "");
  const items = [["API принял платёж: HTTP 202, статус pending.", "ok", at(p.created_at)]];
  s.outbox.forEach((e, i) => {
    items.push([
      i === 0
        ? "Событие payment.created записано в outbox в той же транзакции, что и платёж."
        : "Страховка добавила новое событие: платёж слишком долго оставался pending.",
      "info",
      at(e.created_at),
    ]);
    if (e.attempts > 0) {
      items.push([`Relay не смог опубликовать событие ${e.attempts} раз: ${e.last_error}`, "warn", ""]);
    }
    items.push(
      e.published_at
        ? ["Relay опубликовал событие в RabbitMQ, брокер подтвердил приём.", "ok", at(e.published_at)]
        : ["Событие ждёт, пока relay его опубликует.", "info", ""],
    );
  });
  if (p.status === "succeeded") {
    items.push(["Шлюз одобрил платёж, статус succeeded.", "ok", at(p.processed_at)]);
  } else if (p.status === "failed") {
    items.push([
      `Шлюз отказал (${p.failure_reason}). Это обычный исход, а не сбой: статус failed, ` +
        "повторять нечего, клиенту уходит вебхук.",
      "bad",
      at(p.processed_at),
    ]);
  } else if (s.outbox.some((e) => e.published_at)) {
    items.push(["Consumer получил событие и ждёт ответ шлюза (2-5 с).", "info", ""]);
  }
  for (const hook of s.hooks) {
    if (hook.answered === 200) {
      items.push(["Сервер клиента принял вебхук (200), подпись HMAC верна.", "ok", at(hook.received_at)]);
    } else {
      const why = hook.signature_valid ? "" : ", подпись не сошлась";
      items.push([
        `Сервер клиента ответил ${hook.answered}${why}: попытка доставки не удалась.`,
        "warn",
        at(hook.received_at),
      ]);
    }
  }
  if (p.webhook_delivered_at) {
    items.push(["Доставка вебхука записана в БД (webhook_delivered_at).", "ok", at(p.webhook_delivered_at)]);
  }
  return items;
}

function renderSnapshot(run, s) {
  const p = s.payment;
  const [label, tone] = paymentLabel(p.status);
  if (p.status !== "pending" && !p.webhook_delivered_at) run.setBadge(`${label}, вебхук не доставлен`, "warn");
  else run.setBadge(label, tone);
  run.body.replaceChildren(
    h(
      "ol",
      { class: "steps" },
      steps(s).map(([text, stepTone, time]) =>
        h(
          "li",
          { class: `step ${toneClass(stepTone)}` },
          h("span", { class: "step-text" }, text),
          time ? h("span", { class: "step-time", title: "От создания платежа, по часам сервера" }, time) : null,
        ),
      ),
    ),
  );
  run.foot.replaceChildren(
    `${fmtMoney(p.amount, p.currency)} · платёж `,
    h("code", null, p.payment_id),
    " · Idempotency-Key ",
    h("code", null, p.idempotency_key),
  );
}

// Log what changed between two snapshots of the same payment.
function logChanges(run, before, after) {
  if (!before) return;
  const published = (s) => s.outbox.filter((e) => e.published_at).length;
  if (after.outbox.length > before.outbox.length) run.log("Страховка добавила новое событие в outbox.");
  if (published(after) > published(before)) run.log("Relay опубликовал событие в RabbitMQ.");
  if (after.payment.status !== before.payment.status) {
    const tone = after.payment.status === "succeeded" ? "good" : "bad";
    run.log(`Платёж: «${paymentLabel(before.payment.status)[0]}» → «${paymentLabel(after.payment.status)[0]}».`, tone);
  }
  for (const hook of after.hooks.slice(before.hooks.length)) {
    run.log(`Вебхук: сервер клиента ответил ${hook.answered}.`, hook.answered === 200 ? "good" : "bad");
  }
  if (after.payment.webhook_delivered_at && !before.payment.webhook_delivered_at) {
    run.log("Вебхук доставлен.", "good");
  }
}

// Re-read the payment every second until `until` says enough. Returns the last snapshot.
async function follow(run, id, { until, tick, status } = {}) {
  const done = until || ((s) => Boolean(s.payment.webhook_delivered_at));
  const start = performance.now();
  const waited = () => (performance.now() - start) / 1000;
  let last = null;
  let problem = null;
  run.busy(() => (status ? status(last, waited()) : `Следим за платежом ${Math.floor(waited())} с…`));
  while (run.alive) {
    let s = null;
    try {
      s = await snapshot(run, id);
    } catch (error) {
      if (problem !== error.message) run.log(`${error.message}. Страница продолжает спрашивать.`, "bad");
      problem = error.message;
    }
    if (!run.alive) return last;
    if (s) {
      problem = null;
      logChanges(run, last, s);
      last = s;
      renderSnapshot(run, s);
      const extra = tick ? await tick(s) : undefined;
      if (done(s, extra)) break;
    }
    if (waited() > WATCH_MAX_SECONDS) {
      run.log(`За ${WATCH_MAX_SECONDS} с не дождались, страница перестала следить.`, "bad");
      break;
    }
    await sleep(1000);
  }
  run.idle();
  return last;
}

function totalSeconds(p) {
  return p.webhook_delivered_at ? fmtSeconds(secondsBetween(p.created_at, p.webhook_delivered_at), 1) : null;
}

// ---------------------------------------------------------------- scenarios

async function runSingle(scenario, run) {
  run.log(`POST /api/v1/payments: ${fmtMoney(AMOUNT, CURRENCY)}, вебхук на ${WEBHOOK_URL}.`);
  const result = await createPayment(run);
  if (!run.alive) return;
  if (result.status !== 202) {
    run.fail(`Ответ API: HTTP ${result.status}, ${errorText(result)}.`);
    return;
  }
  run.log(`Ответ API за ${fmtSeconds(run.elapsed(), 2)} с: HTTP 202, статус ${result.data.status}.`);
  const last = await follow(run, result.data.payment_id);
  if (!run.alive || !last) return;
  const total = totalSeconds(last.payment);
  run.summary.textContent = total
    ? `От запроса до вебхука ${total} с. Сам API ответил сразу, остальное сделали relay и consumer в фоне.`
    : "";
}

async function runSeries(scenario, run) {
  const rows = [];
  const tbody = h("tbody");
  run.extra.replaceChildren(
    h(
      "table",
      { class: "table" },
      h(
        "thead",
        null,
        h("tr", null, h("th", null, "№"), h("th", null, "Платёж"), h("th", { class: "num" }, "Сумма"), h("th", null, "Статус"), h("th", null, "Вебхук")),
      ),
      tbody,
    ),
  );
  for (let i = 0; i < 10; i += 1) {
    const amount = `${100 + i}.00`;
    const result = await createPayment(run, randomKey(), { amount });
    if (!run.alive) return;
    const status = h("span", { class: "badge tone-info" }, "в обработке");
    const hook = h("span", null, "…");
    const id = result.status === 202 ? result.data.payment_id : null;
    tbody.append(
      h("tr", null, h("td", { class: "num" }, i + 1), h("td", null, h("code", null, id ? shortId(id) : `HTTP ${result.status}`)), h("td", { class: "num" }, fmtMoney(amount, CURRENCY)), h("td", null, status), h("td", null, hook)),
    );
    rows.push({ id, status, hook, payment: null });
  }
  const accepted = rows.filter((r) => r.id).length;
  if (accepted === rows.length) run.log("Отправлено 10 платежей, на каждый API ответил 202.");
  else run.log(`Отправлено 10 платежей, 202 получили ${accepted}, остальные см. в таблице.`, "bad");
  const start = performance.now();
  run.busy(() => `Ждём обработку и вебхуки: ${Math.floor((performance.now() - start) / 1000)} с…`);
  while (run.alive && (performance.now() - start) / 1000 < WATCH_MAX_SECONDS) {
    await Promise.all(
      rows
        .filter((r) => r.id && !(r.payment && r.payment.webhook_delivered_at))
        .map(async (r) => {
          const result = await getPayment(run, r.id);
          if (result.status !== 200) return;
          r.payment = result.data;
          const [label, tone] = paymentLabel(r.payment.status);
          r.status.textContent = r.payment.failure_reason ? `${label}: ${r.payment.failure_reason}` : label;
          r.status.className = `badge ${toneClass(tone)}`;
          r.hook.textContent = r.payment.webhook_delivered_at
            ? `доставлен за ${totalSeconds(r.payment)} с`
            : "…";
        }),
    );
    if (rows.every((r) => !r.id || (r.payment && r.payment.webhook_delivered_at))) break;
    await sleep(1000);
  }
  run.idle();
  if (!run.alive) return;
  const settled = rows.filter((r) => r.payment && r.payment.status !== "pending");
  const ok = settled.filter((r) => r.payment.status === "succeeded").length;
  const delivered = rows.filter((r) => r.payment && r.payment.webhook_delivered_at).length;
  run.summary.textContent =
    `Оплачено ${ok}, отказов шлюза ${settled.length - ok}, вебхуков доставлено ${delivered} из 10. ` +
    "Отказ выпадает в среднем в одном платеже из десяти, поэтому в одной серии их бывает и 0, " +
    "и 2-3: это случайность, а не сбой.";
  run.log(`Готово: оплачено ${ok}, отказов ${settled.length - ok}, вебхуков доставлено ${delivered} из 10.`, delivered === 10 ? "good" : "bad");
  run.setBadge(delivered === 10 ? "все уведомлены" : "не все уведомлены", delivered === 10 ? "ok" : "warn");
}

async function runIdempotency(scenario, run) {
  const checks = checkList(run);
  const key = randomKey();
  run.busy(() => "Первый запрос…");
  const first = await createPayment(run, key);
  if (!run.alive) return;
  const id = first.data && first.data.payment_id;
  run.log(`POST с ключом ${key}: HTTP ${first.status}.`);
  checks.add(first.status === 202, `Первый запрос: HTTP ${first.status}, платёж ${shortId(id)}.`);

  run.busy(() => "Тот же запрос ещё раз…");
  const second = await createPayment(run, key);
  if (!run.alive) return;
  const replayed = second.headers.get("Idempotent-Replayed");
  run.log(`Тот же запрос: HTTP ${second.status}, Idempotent-Replayed: ${replayed}.`);
  checks.add(
    second.status === 202 && replayed === "true" && Boolean(second.data) && second.data.payment_id === id,
    `Повтор: HTTP ${second.status}, Idempotent-Replayed: ${replayed}, тот же платёж ${shortId(second.data && second.data.payment_id)}.`,
  );

  run.busy(() => "Пять одновременных запросов с новым ключом…");
  const raceKey = randomKey();
  const race = await Promise.all(Array.from({ length: 5 }, () => createPayment(run, raceKey)));
  if (!run.alive) return;
  const raceIds = new Set(race.map((r) => r.data && r.data.payment_id));
  const fresh = race.filter((r) => r.headers.get("Idempotent-Replayed") !== "true").length;
  const raceId = [...raceIds][0];
  const outbox = raceId ? await getOutbox(run, raceId) : { status: 0, data: [] };
  if (!run.alive) return;
  const events = Array.isArray(outbox.data) ? outbox.data.length : "?";
  run.log(`5 одновременных: ${race.map((r) => r.status).join(", ")}; платежей ${raceIds.size}, событий в outbox ${events}.`);
  checks.add(
    race.every((r) => r.status === 202) && raceIds.size === 1 && fresh === 1 && events === 1,
    `5 одновременных запросов: один платёж ${shortId(raceId)}, новым его назвал один ответ, ` +
      `событий в outbox ${events}. Второй запрос ждёт на уникальном индексе и получает готовый платёж.`,
  );

  run.busy(() => "Тот же ключ, другое тело…");
  const third = await createPayment(run, key, { amount: "999.00" });
  if (!run.alive) return;
  run.log(`Тот же ключ с суммой 999,00: HTTP ${third.status}, ${errorText(third)}.`);
  checks.add(
    third.status === 422 && errorText(third) === "idempotency_key_reused",
    `Тот же ключ с другой суммой: HTTP ${third.status}, ${errorText(third)}. Сервис не угадывает, ` +
      "какой запрос правильный, а отклоняет второй.",
  );
  run.idle();
  run.setBadge(checks.ok ? "всё сходится" : "не сходится", checks.ok ? "ok" : "bad");
}

async function runValidation(scenario, run) {
  const checks = checkList(run);
  const cases = [
    {
      what: "Без X-API-Key",
      send: () => api("POST", "/api/v1/payments", { run, body: paymentBody(), headers: { "Idempotency-Key": randomKey() }, key: null }),
      status: 401,
    },
    {
      what: "Без Idempotency-Key",
      send: () => api("POST", "/api/v1/payments", { run, body: paymentBody() }),
      status: 400,
    },
    {
      what: "Сумма числом с дробью (1500.5)",
      // As raw text: JSON.stringify would keep it a number too, this just makes it explicit.
      send: () =>
        api("POST", "/api/v1/payments", {
          run,
          body: JSON.stringify(paymentBody()).replace('"1500.00"', "1500.5"),
          headers: { "Idempotency-Key": randomKey() },
        }),
      status: 422,
      why: "число с дробью уже float, а float теряет точность; сумму шлют строкой",
    },
    {
      what: "Три знака после запятой (\"10.001\")",
      send: () => createPayment(run, randomKey(), { amount: "10.001" }),
      status: 422,
    },
    {
      what: "Валюта GBP",
      send: () => createPayment(run, randomKey(), { currency: "GBP" }),
      status: 422,
    },
    {
      what: "webhook_url не http(s)",
      send: () => createPayment(run, randomKey(), { webhook_url: "ftp://client.test/hook" }),
      status: 422,
    },
  ];
  for (const c of cases) {
    run.busy(() => `${c.what}…`);
    const result = await c.send();
    if (!run.alive) return;
    run.log(`${c.what}: HTTP ${result.status}, ${errorText(result)}.`);
    const why = c.why ? ` (${c.why})` : "";
    checks.add(result.status === c.status, `${c.what}: HTTP ${result.status}, ${errorText(result)}${why}.`);
  }
  run.idle();
  run.setBadge(checks.ok ? "всё отклонено" : "не сходится", checks.ok ? "ok" : "bad");
}

async function runDlq(scenario, run) {
  const before = await dlqCount(run);
  let id = null;
  let settled = null;
  try {
    const failing = await setReceiverFail(run, true);
    if (failing.status !== 200) {
      run.fail(`Не удалось «уронить» приёмник: HTTP ${failing.status}, ${errorText(failing)}.`);
      return;
    }
    run.log("Приёмник вебхуков теперь отвечает 500 на всё.", "bad");
    refreshReceiver();
    const result = await createPayment(run);
    if (!run.alive) return;
    if (result.status !== 202) {
      run.fail(`Ответ API: HTTP ${result.status}, ${errorText(result)}.`);
      return;
    }
    id = result.data.payment_id;
    run.log("Платёж создан: HTTP 202.");
    let reached = false;
    settled = await follow(run, id, {
      tick: async () => {
        const count = await dlqCount(run);
        reached = before !== null && count !== null && count > before;
        return reached;
      },
      until: (s, inDlq) => Boolean(inDlq) || Boolean(s.payment.webhook_delivered_at),
      status: (s) => {
        const tries = s ? s.hooks.length : 0;
        return `Вебхук не принимается, попыток ${tries} из 3. Паузы между ними 2 и 4 с…`;
      },
    });
    if (!run.alive) return;
    if (!reached) {
      run.summary.textContent =
        "Сообщение не дошло до DLQ за отведённое время. Если цепь breaker уже была разомкнута " +
        "предыдущими неудачами, вебхук ждёт в очереди задержки: см. панель очередей.";
      return;
    }
    run.log("Три попытки не удались: сообщение в DLQ (payments.new.dlq).", "bad");
  } finally {
    await setReceiverFail(run, false);
    refreshReceiver();
  }
  if (!run.alive) return;
  run.log("Приёмник вебхуков снова отвечает 200.", "good");
  run.setBadge("в DLQ", "warn");
  const outcome = settled ? paymentLabel(settled.payment.status)[0] : "обработан";
  run.summary.textContent =
    `Платёж обработан (${outcome}), а вебхук о результате не доставлен: сообщение лежит в DLQ. ` +
    "Сервер клиента починили, теперь сообщение можно вернуть в работу.";
  const button = h("button", { type: "button", class: "action primary" }, "Вернуть из DLQ");
  const actions = h(
    "div",
    { class: "card-actions" },
    button,
    h("span", { class: "hint" }, "Как python -m app.messaging.dlq --replay: вернёт все сообщения из DLQ, не только это."),
  );
  run.extra.replaceChildren(actions);
  button.addEventListener("click", async () => {
    button.disabled = true;
    const replay = await replayDlq(run);
    if (!run.alive) return;
    if (replay.status !== 200) {
      run.fail(`Replay: HTTP ${replay.status}, ${errorText(replay)}.`);
      button.disabled = false;
      return;
    }
    run.log(`Из DLQ возвращено сообщений: ${replay.data.moved}.`, "good");
    refreshQueues();
    const last = await follow(run, id);
    if (!run.alive || !last) return;
    if (last.payment.webhook_delivered_at) {
      actions.remove();
      run.setBadge("вебхук доставлен", "ok");
      run.summary.textContent =
        "Сообщение вернулось из DLQ, consumer увидел, что платёж уже обработан, и только " +
        "отправил вебхук. Шлюз второй раз не вызывался.";
    }
  });
}

async function runBreaker(scenario, run) {
  const counters = {
    requests: h("b", null, "0"),
    failed: h("b", null, "0"),
    postponed: h("b", null, "0"),
    delivered: h("b", null, `0/${BREAKER_PAYMENTS}`),
    dlq: h("b", null, "0"),
  };
  run.extra.replaceChildren(
    h(
      "div",
      { class: "counters" },
      h("span", null, "Запросов до сервера: ", counters.requests),
      h("span", null, "из них неудачных: ", counters.failed),
      h("span", null, "ждут в очереди задержки: ", counters.postponed),
      h("span", null, "доставлено: ", counters.delivered),
      h("span", null, "прибавилось в DLQ: ", counters.dlq),
    ),
  );
  const dlqBefore = await dlqCount(run);
  const ids = [];
  let maxPostponed = 0;
  let failedTotal = 0;
  let deliveredTotal = 0;
  let dlqDelta = 0;

  const update = async () => {
    const [hooks, postponed, dlq, payments] = await Promise.all([
      getHooks(run, null),
      postponedCount(run),
      dlqCount(run),
      Promise.all(ids.map((id) => getPayment(run, id))),
    ]);
    const mine = hooks.status === 200 && Array.isArray(hooks.data) ? hooks.data.filter((x) => ids.includes(x.payment_id)) : [];
    failedTotal = mine.filter((x) => x.answered !== 200).length;
    counters.requests.textContent = String(mine.length);
    counters.failed.textContent = String(failedTotal);
    if (postponed !== null) {
      maxPostponed = Math.max(maxPostponed, postponed);
      counters.postponed.textContent = String(postponed);
    }
    deliveredTotal = payments.filter((p) => p.status === 200 && p.data.webhook_delivered_at).length;
    counters.delivered.textContent = `${deliveredTotal}/${ids.length}`;
    if (dlq !== null && dlqBefore !== null) {
      dlqDelta = Math.max(0, dlq - dlqBefore);
      counters.dlq.textContent = String(dlqDelta);
    }
  };

  try {
    const failing = await setReceiverFail(run, true);
    if (failing.status !== 200) {
      run.fail(`Не удалось «уронить» приёмник: HTTP ${failing.status}, ${errorText(failing)}.`);
      return;
    }
    refreshReceiver();
    run.log(`Приёмник вебхуков лежит (отвечает 500) ${BREAKER_OUTAGE_SECONDS} с.`, "bad");
    for (let i = 0; i < BREAKER_PAYMENTS; i += 1) {
      const result = await createPayment(run, randomKey(), { amount: `${300 + i}.00` });
      if (!run.alive) return;
      if (result.status === 202) ids.push(result.data.payment_id);
    }
    run.log(`Создано платежей: ${ids.length}.`);
    const outageEnds = performance.now() + BREAKER_OUTAGE_SECONDS * 1000;
    run.busy(() => `Сервер лежит ещё ${Math.max(0, Math.ceil((outageEnds - performance.now()) / 1000))} с…`);
    while (run.alive && performance.now() < outageEnds) {
      await update();
      await sleep(1000);
    }
  } finally {
    await setReceiverFail(run, false);
    refreshReceiver();
  }
  if (!run.alive) return;
  await update();
  run.log(
    `Сервер ожил. За время простоя до него дошло ${counters.requests.textContent} запросов на ${ids.length} платежей: ` +
      "дальше цепь разомкнулась, и вебхуки ждали без запросов.",
    "good",
  );
  const start = performance.now();
  run.busy(() => `Ждём пробный запрос и доставку: ${Math.floor((performance.now() - start) / 1000)} с (пауза цепи до 30 с)…`);
  while (run.alive && deliveredTotal < ids.length && (performance.now() - start) / 1000 < WATCH_MAX_SECONDS) {
    await sleep(1000);
    await update();
  }
  run.idle();
  if (!run.alive) return;
  const allDelivered = deliveredTotal === ids.length;
  run.summary.textContent =
    `Доставлено ${deliveredTotal} из ${ids.length}, неудачных запросов ${failedTotal} ` +
    `(без breaker было бы ${ids.length * 3} и ${ids.length} сообщений в DLQ), в DLQ прибавилось ${dlqDelta}. ` +
    `Больше всего вебхуков ждали в очереди задержки одновременно: ${maxPostponed}.`;
  run.setBadge(allDelivered && dlqDelta === 0 ? "все доставлены" : "не сходится", allDelivered && dlqDelta === 0 ? "ok" : "warn");
}

const RUNNERS = {
  single: runSingle,
  series: runSeries,
  idempotency: runIdempotency,
  validation: runValidation,
  dlq: runDlq,
  breaker: runBreaker,
};

function start(scenario) {
  const run = new Run(scenario);
  RUNNERS[scenario.kind](scenario, run)
    .catch((error) => {
      // Last line of defence: never leave a card spinning without an explanation.
      console.error(error);
      if (run.alive) run.fail(`Сбой на странице: ${error.message}`);
    })
    .finally(() => run.done());
}

function clearFeed() {
  for (const run of [...activeRuns]) run.stop();
  $("runs").replaceChildren();
  $("empty").hidden = false;
}

function setActiveButton(button) {
  if (activeButton) {
    activeButton.classList.remove("active");
    activeButton.setAttribute("aria-pressed", "false");
  }
  activeButton = button;
  if (button) {
    button.classList.add("active");
    button.setAttribute("aria-pressed", "true");
  }
}

function selectScenario(button, scenario) {
  if (button !== activeButton) {
    clearFeed();
    setActiveButton(button);
  }
  start(scenario);
}

// ---------------------------------------------------------------- side panels

function setKeyRejected(rejected) {
  if (keyRejected === rejected) return;
  keyRejected = rejected;
  $("key-status").textContent = rejected ? "API отвечает 401: ключ не подходит." : "";
  updateBanner();
}

function updateBanner() {
  const banner = $("banner");
  let text = null;
  if (keyRejected) {
    text = "API не принимает ключ из панели слева (401). Впишите значение API_KEY, с которым запущен стек.";
  } else if (receiverFailing) {
    text =
      "Сервер клиента сейчас отвечает 500 на все вебхуки. Пока он «лежит», вебхуки других " +
      "сценариев тоже не доставляются, а после 5 неудач подряд размыкается цепь breaker.";
  }
  banner.hidden = !text;
  banner.textContent = text || "";
}

async function refreshReceiver() {
  const badge = $("receiver-mode");
  const toggle = $("receiver-toggle");
  const log = $("receiver-log");
  let mode = null;
  let hooks = null;
  try {
    [mode, hooks] = await Promise.all([api("GET", "/demo/api/receiver/mode"), getHooks(null, null)]);
  } catch {
    mode = null;
  }
  if (!mode || mode.status !== 200) {
    badge.textContent = "нет связи";
    badge.className = "badge";
    toggle.disabled = true;
    log.replaceChildren();
    return;
  }
  receiverFailing = Boolean(mode.data.fail);
  badge.textContent = receiverFailing ? "отвечает 500" : "работает";
  badge.className = `badge ${receiverFailing ? "tone-bad" : "tone-ok"}`;
  toggle.disabled = false;
  toggle.textContent = receiverFailing ? "Поднять сервер" : "Уронить сервер";
  const items = hooks && hooks.status === 200 && Array.isArray(hooks.data) ? hooks.data.slice(0, 6) : [];
  log.replaceChildren(
    ...items.map((x) =>
      h(
        "li",
        null,
        h("span", { class: "t" }, clock(x.received_at)),
        h("span", { class: `badge ${x.answered === 200 ? "tone-ok" : "tone-bad"}` }, String(x.answered)),
        h("code", null, x.payment_id || ""),
      ),
    ),
  );
  updateBanner();
}

function queueDescription(q) {
  if (q.role === "main") return "основная, её читает consumer";
  if (q.role === "retry") return `пауза ${fmtSeconds(q.delay_ms / 1000, 0)} с перед повтором`;
  if (q.role === "postpone") return `ожидание, пока цепь разомкнута (${fmtSeconds(q.delay_ms / 1000, 0)} с)`;
  if (q.role === "dlq") return "DLQ: не обработаны после всех попыток";
  return q.role;
}

async function refreshQueues() {
  const box = $("queues");
  let result = null;
  try {
    result = await getQueues(null);
  } catch {
    result = null;
  }
  if (!result || result.status !== 200 || !Array.isArray(result.data)) {
    box.replaceChildren(h("p", { class: "hint" }, result && result.status === 401 ? "Нужен ключ API." : "RabbitMQ не отвечает."));
    $("replay").disabled = true;
    return;
  }
  box.replaceChildren(
    ...result.data.map((q) =>
      h(
        "div",
        { class: "queue" },
        h("div", { class: "queue-name" }, h("code", null, q.name), h("span", null, queueDescription(q))),
        h("span", { class: `badge ${q.messages ? (q.role === "dlq" ? "tone-bad" : "tone-info") : ""}` }, String(q.messages)),
      ),
    ),
  );
  const dlq = result.data.find((q) => q.role === "dlq");
  $("replay").disabled = !dlq || dlq.messages === 0;
}

async function replayFromPanel() {
  const status = $("replay-status");
  $("replay").disabled = true;
  const result = await replayDlq(null);
  status.textContent =
    result.status === 200
      ? `Возвращено в payments.new: ${result.data.moved}. Уже проведённые платежи получат только вебхук.`
      : `Не вышло: HTTP ${result.status}, ${errorText(result)}.`;
  refreshQueues();
}

async function toggleReceiver() {
  $("receiver-toggle").disabled = true;
  await setReceiverFail(null, !receiverFailing);
  refreshReceiver();
}

function refreshSide() {
  refreshReceiver();
  refreshQueues();
}

function initKey() {
  const input = $("api-key");
  let saved = null;
  try {
    saved = localStorage.getItem(KEY_STORAGE);
  } catch {
    saved = null;
  }
  input.value = saved || DEFAULT_KEY;
  input.addEventListener("change", () => {
    try {
      localStorage.setItem(KEY_STORAGE, apiKey());
    } catch {
      // private mode: the key just is not remembered
    }
    keyRejected = false;
    $("key-status").textContent = "";
    updateBanner();
    refreshSide();
  });
}

function init() {
  initKey();
  $("scenarios").replaceChildren(
    ...SCENARIOS.map((s) =>
      h(
        "button",
        {
          type: "button",
          class: "scenario",
          "aria-pressed": "false",
          onclick: (event) => selectScenario(event.currentTarget, s),
        },
        h("b", null, s.title),
        h("span", null, s.what),
      ),
    ),
  );
  $("clear").addEventListener("click", () => {
    clearFeed();
    setActiveButton(null);
  });
  $("receiver-toggle").addEventListener("click", toggleReceiver);
  $("replay").addEventListener("click", replayFromPanel);
  refreshSide();
  setInterval(refreshSide, SIDE_REFRESH_MS);
}

init();
