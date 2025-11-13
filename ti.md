## JS Interview Challenge: Async Rate-Limited Queue

**Контекст.** У вас есть список асинхронных задач (функций), каждая из которых возвращает `Promise`. Нужно выполнить эти задачи, соблюдая ограничение: не более `limit` параллельно запущенных задач и не более `limit` завершённых задач за любой скользящий интервал `windowMs`. Это сочетает ограничения на конкуренцию и rate limiting.

### Требование

Напишите функцию `createLimitedRunner(tasks, { limit, windowMs })`, которая:
- принимает массив `tasks`, где каждая задача — это функция без аргументов, возвращающая `Promise`;
- запускает задачи в порядке их следования;
- гарантирует, что одновременно выполняется не более `limit` задач;
- гарантирует, что за любой интервал длиной `windowMs` завершается не более `limit` задач;
- возвращает `Promise`, который резолвится массивом результатов в исходном порядке;
- при ошибке любой задачи:
  - отменяет запуск новых задач,
  - дожидается завершения уже запущенных,
  - отклоняет общий `Promise` первой полученной ошибкой.

Дополнительно реализуйте `cancel()` для преждевременной остановки обработки. Вызов `cancel()`:
- сразу отклоняет общий `Promise` ошибкой `Error('Cancelled')`,
- не запускает новые задачи,
- не отменяет уже выполняющиеся, но их результаты игнорируются.

### Пример использования

```js
const runner = createLimitedRunner(tasks, { limit: 3, windowMs: 200 });

runner.execute()
  .then(results => console.log(results))
  .catch(err => console.error(err));

setTimeout(() => runner.cancel(), 350);
```

### Ожидаемые навыки

- понимание Event Loop и очереди микротасков;
- управление конкуренцией без сторонних библиотек;
- чистая архитектура, легко покрываемая тестами;
- корректная работа с ошибками и отменой.

### Подсказки для интервьюера

- Попросите кандидата описать архитектуру перед реализацией (очередь задач, окно завершений, состояние).
- Обратите внимание на то, как кандидат тестирует: например, имитация задач с задержками разной длины.
- Дополнительное усложнение: добавьте параметр `timeout` на задачу или обобщите на "динамический" `limit`.

### Референсный каркас (не решение)

```js
export const createLimitedRunner = (tasks, { limit, windowMs }) => {
  if (!Array.isArray(tasks)) throw new TypeError('tasks must be an array');
  if (limit <= 0 || windowMs <= 0) throw new RangeError('limit/windowMs must be > 0');

  let cancelled = false;
  let active = 0;
  const history = []; // здесь удобно хранить timestamps завершений

  const runNext = () => {
    // TODO: запуск следующей задачи с учётом ограничений
  };

  const execute = () => {
    // TODO: основная логика
  };

  const cancel = () => {
    cancelled = true;
    // TODO: завершение общей гонки
  };

  return { execute, cancel };
};
```

### Возможное решение

```js
export const createLimitedRunner = (tasks, { limit, windowMs }) => {
  if (!Array.isArray(tasks)) throw new TypeError('tasks must be an array');
  if (!Number.isFinite(limit) || limit <= 0) throw new RangeError('limit must be > 0');
  if (!Number.isFinite(windowMs) || windowMs <= 0) throw new RangeError('windowMs must be > 0');

  let started = false;
  let done = false;
  let cancelled = false;
  let resolveMain;
  let rejectMain;

  const total = tasks.length;
  const results = new Array(total);
  const history = [];

  let nextIndex = 0;
  let active = 0;
  let settled = 0;
  let firstError;

  const trimHistory = (now) => {
    while (history.length && now - history[0] >= windowMs) {
      history.shift();
    }
  };

  const waitForWindow = () => {
    if (cancelled || done) return Promise.resolve();
    return new Promise((resolve) => {
      const attempt = () => {
        if (cancelled || done) {
          resolve();
          return;
        }
        const now = Date.now();
        trimHistory(now);
        if (history.length < limit) {
          resolve();
          return;
        }
        const delay = Math.max(0, history[0] + windowMs - now);
        setTimeout(attempt, delay);
      };
      attempt();
    });
  };

  const finalize = (applyResult) => {
    const gate = (!cancelled && !done)
      ? waitForWindow().then(() => {
          if (!cancelled && !done) {
            const now = Date.now();
            trimHistory(now);
            history.push(now);
          }
        })
      : Promise.resolve();

    gate
      .then(() => {
        if (!cancelled && !done && typeof applyResult === 'function') {
          applyResult();
        }
      })
      .finally(() => {
        settled += 1;
        maybeComplete();
      });
  };

  const maybeComplete = () => {
    if (!started || done || cancelled) return;
    if (firstError) {
      if (active === 0) {
        done = true;
        rejectMain(firstError);
      }
      return;
    }
    if (settled === total && active === 0 && nextIndex >= total) {
      done = true;
      resolveMain(results);
    }
  };

  function handleSuccess(index, value) {
    active -= 1;
    pump();
    finalize(() => {
      results[index] = value;
    });
  }

  function handleFailure(error) {
    active -= 1;
    if (!firstError) {
      firstError = error;
    }
    pump();
    finalize();
  }

  function startTask(index) {
    active += 1;
    Promise.resolve()
      .then(() => tasks[index]())
      .then(
        (value) => handleSuccess(index, value),
        (error) => handleFailure(error),
      );
  }

  function pump() {
    if (!started || done || cancelled || firstError) return;
    while (active < limit && nextIndex < total) {
      startTask(nextIndex++);
    }
  }

  const execute = () => {
    if (started) throw new Error('execute() already called');
    if (cancelled) throw new Error('Runner already cancelled');

    const invalidIndex = tasks.findIndex((task) => typeof task !== 'function');
    if (invalidIndex !== -1) {
      throw new TypeError(`Task at index ${invalidIndex} must be a function`);
    }

    started = true;
    if (total === 0) {
      done = true;
      return Promise.resolve([]);
    }

    const controller = new Promise((resolve, reject) => {
      resolveMain = resolve;
      rejectMain = reject;
    });

    pump();
    return controller;
  };

  const cancel = () => {
    if (cancelled || done || firstError) return;
    cancelled = true;
    if (!started) {
      done = true;
      return;
    }
    done = true;
    rejectMain(new Error('Cancelled'));
  };

  return { execute, cancel };
};
```

Интервью длится 45–60 минут. Требования можно подстраивать под уровень кандидата.
