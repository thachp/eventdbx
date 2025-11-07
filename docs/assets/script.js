document.addEventListener("DOMContentLoaded", () => {
  const toggle = document.querySelector("[data-nav-toggle]");
  const nav = document.querySelector("[data-mobile-nav]");

  if (!toggle || !nav) {
    return;
  }

  const isMobile = () => window.innerWidth < 768;

  const isNavOpen = () => !nav.classList.contains("hidden");

  const openNav = () => {
    nav.classList.remove("hidden");
    nav.classList.add("flex");
    toggle.setAttribute("aria-expanded", "true");
  };

  const closeNav = () => {
    nav.classList.add("hidden");
    nav.classList.remove("flex");
    toggle.setAttribute("aria-expanded", "false");
  };

  const syncNavToViewport = () => {
    if (window.innerWidth >= 768) {
      nav.classList.add("flex");
      nav.classList.remove("hidden");
      toggle.setAttribute("aria-expanded", "true");
    } else {
      closeNav();
    }
  };

  toggle.addEventListener("click", () => {
    if (nav.classList.contains("hidden")) {
      openNav();
    } else {
      closeNav();
    }
  });

  nav.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", () => {
      if (isMobile()) {
        closeNav();
      }
    });
  });

  window.addEventListener("resize", syncNavToViewport);

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isMobile() && isNavOpen()) {
      closeNav();
    }
  });

  document.addEventListener("click", (event) => {
    if (!isMobile() || !isNavOpen()) {
      return;
    }
    if (nav.contains(event.target) || toggle.contains(event.target)) {
      return;
    }
    closeNav();
  });

  syncNavToViewport();
});
