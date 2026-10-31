// Follows running tasks in the conversation. A stream replays from the start on
// every (re)connect, so the output is cleared when it opens. The reply form shows
// once nothing is running.
(() => {
  const reply = document.getElementById("reply");
  const atBottom = () => window.innerHeight + window.scrollY >= document.body.offsetHeight - 40;

  if (!location.hash && document.querySelector(".turn")) window.scrollTo(0, document.body.scrollHeight);

  if (reply) {
    const text = reply.querySelector("textarea");
    text.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) reply.requestSubmit();
    });
    reply.addEventListener("submit", () => { reply.querySelector("button").disabled = true; });
    if (!reply.hidden && document.querySelector(".turn")) text.focus();
  }

  for (const out of document.querySelectorAll("[data-stream]")) follow(out);

  function follow(out) {
    const turn = out.closest(".turn");
    const status = turn.querySelector(".task-status");
    const result = turn.querySelector(".task-result");
    const es = new EventSource(out.dataset.stream);

    es.addEventListener("open", () => { out.textContent = ""; });
    es.addEventListener("output", (e) => {
      const follow = atBottom();
      out.textContent += JSON.parse(e.data).chunk;
      if (follow) window.scrollTo(0, document.body.scrollHeight);
    });
    es.addEventListener("done", (e) => {
      es.close();
      delete out.dataset.stream;
      const task = JSON.parse(e.data);
      const pill = document.createElement("span");
      pill.className = "pill " + (task.status === "succeeded" ? "ok" : "bad");
      pill.textContent = task.status;
      status.replaceChildren(pill);
      const parts = [];
      if (task.exitCode !== undefined && task.status !== "succeeded") parts.push("exit code " + task.exitCode);
      if (task.error) parts.push(task.error);
      if (parts.length) result.textContent += " · " + parts.join(" · ");
      if (reply && !document.querySelector("[data-stream]")) {
        reply.hidden = false;
        reply.querySelector("textarea").focus();
        window.scrollTo(0, document.body.scrollHeight);
      }
    });
  }
})();
