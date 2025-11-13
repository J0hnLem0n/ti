## JS Interview Challenge Series: Управление асинхронными задачами

**Формат.** Предложите кандидату серию задач на JavaScript, где каждая следующая опирается на предыдущую. Так вы проверите понимание Event Loop, промисов и организацию кода под рост требований.

---

### Задача 1. Параллельный запуск с ограничением

**Контекст.** Есть массив функций, возвращающих `Promise`. Нужно выполнить их с ограничением на количество одновременно запущенных задач.

#### Требование

- Реализуйте `runWithConcurrency(tasks, limit)`.
- `tasks` — массив функций без аргументов, каждая возвращает `Promise`.
- В любой момент времени выполняется не более `limit` задач.
- Результат — `Promise`, который резолвится массивом значений в порядке исходных задач.
- При первой ошибке немедленно отклоняем общий `Promise` и не запускаем новые задачи.

#### Возможное решение

```js
export const runWithConcurrency = (tasks, limit) => {
  if (!Array.isArray(tasks)) throw new TypeError('tasks must be an array');
  if (!Number.isFinite(limit) || limit <= 0) throw new RangeError('limit must be > 0');

  const invalidIndex = tasks.findIndex((task) => typeof task !== 'function');
  if (invalidIndex !== -1) {
    throw new TypeError(`Task at index ${invalidIndex} must be a function`);
  }

  const total = tasks.length;
  if (total === 0) return Promise.resolve([]);

  let nextIndex = 0;
  let active = 0;
  let completed = 0;
  let rejected = false;
  const results = new Array(total);

  return new Promise((resolve, reject) => {
    function start(index) {
      active += 1;
      Promise.resolve()
        .then(() => tasks[index]())
        .then((value) => {
          results[index] = value;
          active -= 1;
          completed += 1;
          if (completed === total) {
            resolve(results);
          } else {
            launch();
          }
        })
        .catch((error) => {
          if (!rejected) {
            rejected = true;
            reject(error);
          }
        });
    }

    function launch() {
      if (rejected) return;
      while (active < limit && nextIndex < total) {
        start(nextIndex++);
      }
    }

    launch();
  });
};
```

#### Подсказки интервьюеру

- Спросите, как кандидат гарантирует сохранение исходного порядка результатов.
- Предложите протестировать решение с задачами разной длительности и ограничением `limit = 1`.

---

### Задача 2. Управляемый раннер с отменой

**Контекст.** Расширим предыдущую задачу: теперь нужен раннер, которым можно управлять `.execute()` и `.cancel()`, плюс корректная обработка ошибок.

#### Требование

- Реализуйте `createConcurrentRunner(tasks, { limit })`.
- Метод `execute()` запускает задачи, возвращает `Promise` с результатами в исходном порядке.
- Метод `cancel()`:
  - отклоняет общий `Promise` ошибкой `Error('Cancelled')`,
  - запрещает запуск новых задач,
  - не прерывает уже выполняющиеся, но их результаты игнорируются.
- При первой ошибке задачи не запускаем новые, дожидаемся активных и отклоняем общий `Promise` этой ошибкой.

#### Возможное решение

```js
export const createConcurrentRunner = (tasks, { limit }) => {
  if (!Array.isArray(tasks)) throw new TypeError('tasks must be an array');
  if (!Number.isFinite(limit) || limit <= 0) throw new RangeError('limit must be > 0');

  const invalidIndex = tasks.findIndex((task) => typeof task !== 'function');
  if (invalidIndex !== -1) {
    throw new TypeError(`Task at index ${invalidIndex} must be a function`);
  }

  const total = tasks.length;
  const results = new Array(total);

  let started = false;
  let done = false;
  let cancelled = false;
  let resolveMain;
  let rejectMain;

  let nextIndex = 0;
  let active = 0;
  let completed = 0;
  let firstError;

  function maybeFinish() {
    if (!started || done) return;
    if (firstError) {
      if (active === 0) {
        done = true;
        rejectMain(firstError);
      }
      return;
    }
    if (completed === total && active === 0) {
      done = true;
      resolveMain(results);
    }
  }

  function handleSuccess(index, value) {
    results[index] = value;
    active -= 1;
    completed += 1;
    pump();
    maybeFinish();
  }

  function handleFailure(error) {
    active -= 1;
    if (!firstError) {
      firstError = error;
    }
    pump();
    maybeFinish();
  }

  function start(index) {
    active += 1;
    Promise.resolve()
      .then(() => tasks[index]())
      .then((value) => handleSuccess(index, value))
      .catch((error) => handleFailure(error));
  }

  function pump() {
    if (!started || done || cancelled || firstError) return;
    while (active < limit && nextIndex < total) {
      start(nextIndex++);
    }
  }

  const execute = () => {
    if (started) throw new Error('execute() already called');
    if (cancelled) throw new Error('Runner already cancelled');

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
    if (cancelled || done) return;
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

#### Подсказки интервьюеру

- Проверьте, что кандидат умеет корректно обрабатывать ошибку до момента, когда все активные задачи завершатся.
- Попросите объяснить, что произойдёт при повторном вызове `execute()` или `cancel()` и почему.

---

### Задача 3. Rate limiting завершений

**Контекст.** Добавляем ограничение на количество завершённых задач за любой интервал `windowMs`, сохраняя управление раннером и отмену.

#### Требование

- Реализуйте `createLimitedRunner(tasks, { limit, windowMs })`.
- Одновременно выполняется не более `limit` задач.
- За любой интервал длиной `windowMs` завершается не более `limit` задач (успешных или ошибочных).
- При ошибке:
  - новые задачи не запускаем,
  - дожидаемся оставшихся,
  - отклоняем общий `Promise` первой ошибкой.
- Метод `cancel()` немедленно отклоняет общий `Promise` `Error('Cancelled')`, запрещает запуск новых задач и игнорирует результаты уже выполняющихся.

#### Возможное решение

```js
export const createLimitedRunner = (tasks, { limit, windowMs }) => {
  if (!Array.isArray(tasks)) throw new TypeError('tasks must be an array');
  if (!Number.isFinite(limit) || limit <= 0) throw new RangeError('limit must be > 0');
  if (!Number.isFinite(windowMs) || windowMs <= 0) throw new RangeError('windowMs must be > 0');

  const invalidIndex = tasks.findIndex((task) => typeof task !== 'function');
  if (invalidIndex !== -1) {
    throw new TypeError(`Task at index ${invalidIndex} must be a function`);
  }

  const total = tasks.length;
  const results = new Array(total);
  const history = [];

  let started = false;
  let done = false;
  let cancelled = false;
  let resolveMain;
  let rejectMain;

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

  const recordCompletion = async (applyResult) => {
    try {
      if (cancelled || done) return;
      await waitForWindow();
      if (cancelled || done) return;
      const now = Date.now();
      trimHistory(now);
      history.push(now);
      if (typeof applyResult === 'function') applyResult();
    } finally {
      settled += 1;
      maybeFinish();
    }
  };

  const maybeFinish = () => {
    if (!started || done) return;
    if (firstError) {
      if (active === 0) {
        done = true;
        rejectMain(firstError);
      }
      return;
    }
    if (settled === total && active === 0) {
      done = true;
      resolveMain(results);
    }
  };

  const handleSuccess = (index, value) => {
    active -= 1;
    pump();
    recordCompletion(() => {
      results[index] = value;
    });
  };

  const handleFailure = (error) => {
    active -= 1;
    if (!firstError) {
      firstError = error;
    }
    pump();
    recordCompletion();
  };

  const startTask = (index) => {
    active += 1;
    Promise.resolve()
      .then(() => tasks[index]())
      .then((value) => handleSuccess(index, value))
      .catch((error) => handleFailure(error));
  };

  const pump = () => {
    if (!started || done || cancelled || firstError) return;
    while (active < limit && nextIndex < total) {
      startTask(nextIndex++);
    }
  };

  const execute = () => {
    if (started) throw new Error('execute() already called');
    if (cancelled) throw new Error('Runner already cancelled');

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
    if (cancelled || done) return;
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

#### Подсказки интервьюеру

- Попросите кандидата пояснить, почему rate limiting считается по моменту завершения, а не старта задач.
- Обсудите, как решение поведёт себя при череде очень быстрых задач и какие проверки можно добавить в тестах.

---

Интервью можно растянуть на 45–60 минут: начинайте с быстрой оценки по задаче 1, а затем усложняйте до задач 2 и 3 в зависимости от уровня кандидата. При желании можно добавить вариации (таймауты, динамический `limit`, метрики) как дополнительный этап. 
