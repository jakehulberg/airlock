// Follows a running task's output. The stream replays from the start on every
// (re)connect, so the output is cleared when it opens.
(() => {
  const out = document.getElementById("output");
  const url = out && out.dataset.stream;
  if (!url) return;

  const status = document.getElementById("status");
  const result = document.getElementById("result");
  const atBottom = () => window.innerHeight + window.scrollY >= document.body.offsetHeight - 40;
  const es = new EventSource(url);

  es.addEventListener("open", () => { out.textContent = ""; });
  es.addEventListener("output", (e) => {
    const follow = atBottom();
    out.textContent += JSON.parse(e.data).chunk;
    if (follow) window.scrollTo(0, document.body.scrollHeight);
  });
  es.addEventListener("done", (e) => {
    es.close();
    const task = JSON.parse(e.data);
    const pill = document.createElement("span");
    pill.className = "pill " + (task.status === "succeeded" ? "ok" : "bad");
    pill.textContent = task.status;
    status.replaceChildren(pill);
    const parts = [];
    if (task.exitCode !== undefined) parts.push("exit code " + task.exitCode);
    if (task.error) parts.push(task.error);
    if (parts.length) result.textContent += " · " + parts.join(" · ");
  });
})();
