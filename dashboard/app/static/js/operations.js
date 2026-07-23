(() => {
  const body = document.body;

  const showToast = (message, tone = "success") => {
    let region = document.querySelector(".ops-toast-region");
    if (!region) {
      region = document.createElement("div");
      region.className = "ops-toast-region";
      document.body.append(region);
    }
    const toast = document.createElement("div");
    toast.className = `ops-toast ops-toast-${tone}`;
    toast.textContent = message;
    region.append(toast);
    requestAnimationFrame(() => toast.classList.add("is-visible"));
    window.setTimeout(() => {
      toast.classList.remove("is-visible");
      window.setTimeout(() => toast.remove(), 180);
    }, 3200);
  };

  const openModal = (id) => {
    const modal = document.getElementById(id);
    if (!modal) return;
    modal.classList.add("is-open");
    modal.setAttribute("aria-hidden", "false");
    body.classList.add("has-open-modal");
    window.setTimeout(() => modal.querySelector("input:not([type=hidden]), select")?.focus(), 30);
  };

  const closeModal = (modal) => {
    const target = modal?.closest?.(".ops-modal") || modal;
    if (!target) return;
    target.classList.remove("is-open");
    target.setAttribute("aria-hidden", "true");
    if (!document.querySelector(".ops-modal.is-open")) body.classList.remove("has-open-modal");
  };

  const preservedDetails = () =>
    [...document.querySelectorAll("details[open][data-detail-key]")].map((detail) => detail.dataset.detailKey);

  const refreshLivePage = async () => {
    const current = document.querySelector("[data-live-page]");
    if (!current) return;
    const openKeys = preservedDetails();
    const response = await fetch(window.location.href, { headers: { "X-Partial-Page": "1" } });
    if (!response.ok) throw new Error("화면 갱신에 실패했습니다.");
    const html = await response.text();
    const parsed = new DOMParser().parseFromString(html, "text/html");
    const next = parsed.querySelector("[data-live-page]");
    if (!next) throw new Error("갱신할 화면을 찾지 못했습니다.");
    current.innerHTML = next.innerHTML;
    openKeys.forEach((key) => {
      const detail = [...current.querySelectorAll("details[data-detail-key]")].find(
        (candidate) => candidate.dataset.detailKey === key,
      );
      if (detail) detail.open = true;
    });
  };

  const asyncSubmit = async (form) => {
    const confirmation = form.dataset.confirm;
    if (confirmation && !window.confirm(confirmation)) return;
    const submitter = form.querySelector("button[type=submit]");
    const originalText = submitter?.textContent;
    if (submitter) {
      submitter.disabled = true;
      submitter.textContent = "처리 중";
    }
    try {
      const response = await fetch(form.action, {
        method: (form.method || "POST").toUpperCase(),
        body: new FormData(form),
        headers: { Accept: "application/json" },
      });
      const payload = await response.json().catch(() => ({ ok: false, message: "서버 응답을 확인하지 못했습니다." }));
      if (!response.ok || !payload.ok) throw new Error(payload.message || "작업을 완료하지 못했습니다.");
      closeModal(form);
      showToast(payload.message || "처리했습니다.");
      await refreshLivePage();
    } catch (error) {
      showToast(error.message || "작업 중 오류가 발생했습니다.", "error");
    } finally {
      if (submitter) {
        submitter.disabled = false;
        submitter.textContent = originalText;
      }
    }
  };

  const filterBuyerUsers = (form) => {
    const allianceId = form.querySelector("[data-buyer-alliance]")?.value || "";
    const userSelect = form.querySelector("[data-buyer-user]");
    if (!userSelect) return;
    [...userSelect.options].forEach((option, index) => {
      if (index === 0) return;
      option.hidden = option.dataset.allianceId !== allianceId;
      option.disabled = option.hidden;
    });
    if (userSelect.selectedOptions[0]?.disabled) userSelect.value = "";
  };

  const updateSalePreview = (form) => {
    const cash = Number(form.dataset.cashPrice || 0);
    const rate = Number(form.querySelector("[data-sale-rate]")?.value || 0);
    const adena = rate > 0 ? Math.floor((cash * 10000) / rate) : 0;
    const output = form.closest(".ops-modal")?.querySelector("[data-sale-adena]");
    if (output) output.textContent = `${adena.toLocaleString("ko-KR")} 아데나`;
  };

  const populateDropEdit = (button) => {
    const drop = JSON.parse(button.dataset.drop);
    const form = document.querySelector("[data-drop-edit-form]");
    form.action = `/api/drops/${drop.drop_id}`;
    form.elements.attendance_id.value = String(drop.attendance_id);
    form.elements.item_id.value = String(drop.item_id);
    const excluded = new Set((drop.excluded_alliance_ids || []).map(String));
    form.querySelectorAll('input[name="excluded_alliance_ids"]').forEach((checkbox) => {
      checkbox.checked = excluded.has(checkbox.value);
    });
    openModal("drop-edit-modal");
  };

  const populateSale = (button) => {
    const drop = JSON.parse(button.dataset.drop);
    const modal = document.getElementById("drop-sale-modal");
    const form = modal.querySelector("[data-sale-form]");
    form.action = `/api/drops/${drop.drop_id}/sale`;
    form.dataset.cashPrice = String(Number(drop.cash_price_krw || 0));
    form.elements.buyer_alliance_id.value = drop.buyer_alliance_id ? String(drop.buyer_alliance_id) : "";
    form.elements.adena_market_rate.value = Number(drop.adena_market_rate || 0) > 1 ? drop.adena_market_rate : "";
    modal.querySelector("[data-sale-item]").textContent = `${drop.item_name} · 출석 #${drop.attendance_id}`;
    modal.querySelector("[data-sale-cash-label]").textContent = `${Number(drop.cash_price_krw || 0).toLocaleString("ko-KR")}원`;
    filterBuyerUsers(form);
    form.elements.buyer_user_id.value = drop.buyer_user_id ? String(drop.buyer_user_id) : "";
    updateSalePreview(form);
    openModal("drop-sale-modal");
  };

  const populateFeeEdit = (button) => {
    const fee = JSON.parse(button.dataset.fee);
    const form = document.querySelector("[data-fee-edit-form]");
    form.action = `/api/fee-rules/${fee.fee_rule_id}`;
    form.elements.rule_name.value = fee.rule_name;
    form.elements.percent.value = Number(fee.rate_ppm || 0) / 10000;
    form.elements.is_active.checked = Boolean(fee.is_active);
    openModal("fee-edit-modal");
  };

  const populateBidEdit = (button) => {
    const bid = JSON.parse(button.dataset.bid);
    const form = document.querySelector("[data-bid-edit-form]");
    form.action = `/api/bid-items/${bid.bid_item_id}`;
    form.dataset.bidItemId = String(bid.bid_item_id);
    form.elements.item_name.value = bid.item_name;
    form.elements.is_free.checked = Boolean(bid.is_free);
    form.elements.is_active.checked = Boolean(bid.is_active);
    openModal("bid-edit-modal");
  };

  const deleteBidItem = async (button) => {
    const form = button.closest("[data-bid-edit-form]");
    if (!window.confirm("입찰 아이템과 과거 완료 기록을 함께 삭제하시겠습니까?")) return;
    const data = new FormData();
    data.set("guild_id", form.elements.guild_id.value);
    try {
      const response = await fetch(`/api/bid-items/${form.dataset.bidItemId}/delete`, { method: "POST", body: data });
      const payload = await response.json();
      if (!response.ok || !payload.ok) throw new Error(payload.message);
      closeModal(form);
      showToast(payload.message);
      await refreshLivePage();
    } catch (error) {
      showToast(error.message || "삭제하지 못했습니다.", "error");
    }
  };

  document.addEventListener("submit", (event) => {
    const form = event.target.closest("[data-async-form]");
    if (!form) return;
    event.preventDefault();
    event.stopPropagation();
    asyncSubmit(form);
  });

  document.addEventListener("click", (event) => {
    if (event.target.closest("summary form, summary button")) event.stopPropagation();
    const modalOpen = event.target.closest("[data-modal-open]");
    if (modalOpen) openModal(modalOpen.dataset.modalOpen);
    const modalClose = event.target.closest("[data-modal-close]");
    if (modalClose) closeModal(modalClose);
    const dropEdit = event.target.closest("[data-drop-edit]");
    if (dropEdit) populateDropEdit(dropEdit);
    const saleOpen = event.target.closest("[data-sale-open]");
    if (saleOpen) populateSale(saleOpen);
    const feeEdit = event.target.closest("[data-fee-edit]");
    if (feeEdit) populateFeeEdit(feeEdit);
    const bidEdit = event.target.closest("[data-bid-edit]");
    if (bidEdit) populateBidEdit(bidEdit);
    const bidDelete = event.target.closest("[data-bid-delete]");
    if (bidDelete) deleteBidItem(bidDelete);

    const statusFilter = event.target.closest("[data-personal-filter] button");
    if (statusFilter) {
      const filter = statusFilter.dataset.status;
      statusFilter.parentElement.querySelectorAll("button").forEach((button) => button.classList.toggle("is-active", button === statusFilter));
      document.querySelectorAll("[data-personal-status]").forEach((card) => {
        card.hidden = filter !== "all" && card.dataset.personalStatus !== filter;
      });
    }
  });

  document.addEventListener("change", (event) => {
    const buyerAlliance = event.target.closest("[data-buyer-alliance]");
    if (buyerAlliance) filterBuyerUsers(buyerAlliance.form);
  });

  document.addEventListener("input", (event) => {
    const saleInput = event.target.closest("[data-sale-rate]");
    if (saleInput) updateSalePreview(saleInput.form);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeModal(document.querySelector(".ops-modal.is-open"));
  });
})();
