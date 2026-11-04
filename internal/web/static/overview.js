// Refresh only the environment list, leaving the creation form untouched.
(() => {
  async function refresh() {
    try {
      const response = await fetch(location.pathname, { headers: { Accept: "text/html" } });
      if (!response.ok || response.redirected) throw new Error("Unavailable");
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const list = page.getElementById("sandbox-list");
      if (!list) throw new Error("Unavailable");
      // Preserve keyboard focus while a user is navigating the list.
      if (!document.getElementById("sandbox-list").contains(document.activeElement)) {
        document.getElementById("sandbox-list").replaceWith(list);
        document.querySelector(".count").textContent = page.querySelector(".count").textContent;
      }
      document.getElementById("refresh-status").textContent = "Updated just now";
    } catch {
      document.getElementById("refresh-status").textContent = "Updates unavailable. Retrying…";
    }
    setTimeout(refresh, 5000);
  }
  setTimeout(refresh, 5000);
})();
