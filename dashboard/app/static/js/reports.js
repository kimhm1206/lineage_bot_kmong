(() => {
  const page = document.querySelector("[data-report-page]");
  if (!page) return;

  const form = page.querySelector("#report-form");
  const message = page.querySelector("[data-report-message]");
  const previewOutput = page.querySelector("[data-report-preview-output]");
  const reportData = JSON.parse(document.getElementById("report-settings-data")?.textContent || "[]");
  const reportLookup = new Map(reportData.map((report) => [String(report.report_setting_id), report]));

  const showMessage = (text, isError = false) => {
    message.textContent = text || "";
    message.classList.toggle("is-error", isError);
  };

  const postForm = async (url, body) => {
    const response = await fetch(url, {
      method: "POST",
      body,
      headers: { Accept: "application/json" },
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || payload.detail || "요청을 처리하지 못했습니다.");
    }
    return payload;
  };

  const resetForm = () => {
    form.reset();
    form.elements.report_setting_id.value = "";
    form.elements.group_header.value = "";
    form.elements.row_template.value = "";
    form.elements.empty_text.value = "";
    page.querySelector("[data-report-submit-label]").textContent = "알림 저장";
    showMessage("");
  };

  const fillForm = (reportId) => {
    const report = reportLookup.get(String(reportId));
    if (!report) return;
    Object.entries(report).forEach(([name, value]) => {
      if (form.elements[name]) form.elements[name].value = value ?? "";
    });
    page.querySelector("[data-report-submit-label]").textContent = "수정 저장";
    form.scrollIntoView({ behavior: "smooth", block: "start" });
    showMessage("선택한 알림을 편집 중입니다.");
  };

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const submit = form.querySelector('button[type="submit"]');
    submit.disabled = true;
    showMessage("알림을 저장하는 중입니다.");
    try {
      const payload = await postForm("/api/reports", new FormData(form));
      showMessage(payload.message);
      window.location.reload();
    } catch (error) {
      showMessage(error.message, true);
    } finally {
      submit.disabled = false;
    }
  });

  page.addEventListener("click", async (event) => {
    const reset = event.target.closest("[data-report-reset]");
    if (reset) {
      resetForm();
      return;
    }
    const preview = event.target.closest("[data-report-preview]");
    if (preview) {
      preview.disabled = true;
      previewOutput.textContent = "미리보기를 계산하는 중입니다.";
      try {
        const payload = await postForm("/api/reports/preview", new FormData(form));
        previewOutput.textContent = payload.preview;
      } catch (error) {
        previewOutput.textContent = error.message;
      } finally {
        preview.disabled = false;
      }
      return;
    }
    const edit = event.target.closest("[data-report-edit]");
    if (edit) {
      fillForm(edit.dataset.reportEdit);
      return;
    }
    const statusButton = event.target.closest("[data-report-status]");
    if (statusButton) {
      statusButton.disabled = true;
      const body = new FormData();
      body.set("guild_id", form.elements.guild_id.value);
      body.set("status", statusButton.dataset.nextStatus);
      try {
        await postForm(`/api/reports/${statusButton.dataset.reportStatus}/status`, body);
        window.location.reload();
      } catch (error) {
        showMessage(error.message, true);
        statusButton.disabled = false;
      }
      return;
    }
    const deleteButton = event.target.closest("[data-report-delete]");
    if (deleteButton) {
      if (!window.confirm("이 알림을 삭제하시겠습니까?")) return;
      deleteButton.disabled = true;
      const body = new FormData();
      body.set("guild_id", form.elements.guild_id.value);
      body.set("status", "delete");
      try {
        await postForm(`/api/reports/${deleteButton.dataset.reportDelete}/status`, body);
        window.location.reload();
      } catch (error) {
        showMessage(error.message, true);
        deleteButton.disabled = false;
      }
    }
  });
})();
