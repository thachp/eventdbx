document.addEventListener("DOMContentLoaded", () => {
  const toggle = document.querySelector("[data-nav-toggle]");
  const nav = document.querySelector("[data-mobile-nav]");

  if (!toggle || !nav) {
    return;
  }

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
      if (window.innerWidth < 768) {
        closeNav();
      }
    });
  });

  window.addEventListener("resize", syncNavToViewport);
  syncNavToViewport();
});
