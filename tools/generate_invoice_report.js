ObjC.import("Foundation");

function readText(path) {
  return ObjC.unwrap($.NSString.stringWithContentsOfFileEncodingError($(path), $.NSUTF8StringEncoding, null));
}

function writeText(path, text) {
  const string = $.NSString.alloc.initWithUTF8String(String(text));
  string.writeToFileAtomicallyEncodingError($(path), true, $.NSUTF8StringEncoding, null);
}

function sanitizeModule(source) {
  return source
    .replace(/^import[\s\S]*?;\n/gm, "")
    .replace(/^export\s+(const|function|class)\s+/gm, "$1 ")
    .replace(/^export\s+\{[\s\S]*?\};?\n?/gm, "");
}

function loadWorkbook(baseDir) {
  const lookerSource = sanitizeModule(readText(baseDir + "/looker-import.js"));
  const lookerScope = new Function(lookerSource + "\nreturn { importedLookerData };")();
  const dataSource = sanitizeModule(readText(baseDir + "/data.js"));
  return new Function(
    "importedLookerData",
    dataSource + "\nreturn { createInitialWorkbookData, getCorridor, MAJORS, MINORS, TERTIARY };"
  )(lookerScope.importedLookerData);
}

const baseDir = "/Users/danielsinukoff/Documents/billing-workbook";
const outputDir = baseDir + "/reports/looker_import";
const dataScope = loadWorkbook(baseDir);
const workbook = dataScope.createInitialWorkbookData();
const importBundle = new Function(sanitizeModule(readText(baseDir + "/looker-import.js")) + "\nreturn importedLookerData;")();
const period = importBundle.period || "2026-02";
const MAJORS = dataScope.MAJORS;
const MINORS = dataScope.MINORS;
const TERTIARY = dataScope.TERTIARY;

function fmt(n) {
  return (n == null || Number.isNaN(Number(n)) ? "$0.00" : "$" + Number(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }));
}

function fmtPct(n) {
  return (Number(n) * 100).toFixed(4) + "%";
}

function norm(value) {
  return String(value == null ? "" : value).trim().toLowerCase();
}

function optionalMatch(ruleValue, actualValue) {
  return !ruleValue || norm(ruleValue) === norm(actualValue);
}

function inRange(dateValue, start, end) {
  if (!dateValue || !start) return true;
  const date = new Date(dateValue);
  const startDate = new Date(start);
  if (date < startDate) return false;
  if (end) {
    const endDate = new Date(end);
    if (date > endDate) return false;
  }
  return true;
}

function getProductType(txn, rate) {
  if (rate && rate.ccyGroup === "GBP" && !rate.txnType) return "GBP 0.7%";
  if (rate && rate.speedFlag === "RTP") return "RTP";
  if (rate && rate.speedFlag === "FasterACH") return "FasterACH";
  if (txn.speedFlag === "RTP" || (rate && rate.speedFlag === "RTP")) return "RTP";
  if (txn.speedFlag === "FasterACH" || (rate && rate.speedFlag === "FasterACH")) return "FasterACH";
  if (txn.processingMethod === "Wire" || (rate && rate.txnType === "FX" && rate.processingMethod === "Wire")) return "Wire";
  if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Credit" && rate.txnType === "FX") return "Card Credit FX";
  if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Credit") return "Card Credit Domestic";
  if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Debit" && rate.txnType === "FX") return "Card Debit FX";
  if (rate && rate.payerFunding === "Card" && rate.payeeCardType === "Debit") return "Card Debit Domestic";
  if (rate && rate.payeeFunding === "Card" && rate.payeeCardType === "Debit") return "Push-to-Debit";
  if (rate && rate.txnType === "FX") {
    const corridor = rate.ccyGroup === MAJORS ? "Major" : rate.ccyGroup === MINORS ? "Minor" : rate.ccyGroup === TERTIARY ? "Tertiary" : dataScope.getCorridor(rate.ccyGroup || "");
    if (corridor === "Major") return "FX Majors";
    if (corridor === "Minor") return "FX Minors";
    if (corridor === "Tertiary") return "FX Tertiary";
    return "FX Majors";
  }
  return "ACH";
}

function applyFeeCaps(partner, productType, feePerTxn, txnCount) {
  const maxCap = workbook.cap.find((row) => row.partner === partner && row.productType === productType && row.capType === "Max Fee");
  const minCap = workbook.cap.find((row) => row.partner === partner && row.productType === productType && row.capType === "Min Fee");
  let adjFee = feePerTxn;
  let capNote = "";
  if (maxCap && adjFee > maxCap.amount) {
    adjFee = maxCap.amount;
    capNote = " MAX@" + fmt(maxCap.amount) + "/txn";
  }
  if (minCap && adjFee < minCap.amount) {
    adjFee = minCap.amount;
    capNote = " MIN@" + fmt(minCap.amount) + "/txn";
  }
  return { adjFee: adjFee, total: adjFee * txnCount, capNote: capNote, capped: capNote !== "" };
}

function calculateActiveInvoiceTotals(lines) {
  return lines.reduce((totals, line) => {
    if (line.active === false) return totals;
    if (line.dir === "charge") totals.chg += Number(line.amount || 0);
    if (line.dir === "pay") totals.pay += Number(line.amount || 0);
    if (line.dir === "offset") {
      totals.offset += Number(line.amount || 0);
      totals.pay += Number(line.amount || 0);
    }
    return totals;
  }, { chg: 0, pay: 0, offset: 0 });
}

function calculateInvoice(partner, month) {
  const lines = [];
  const notes = [];
  const txns = workbook.ltxn.filter((row) => row.partner === partner && row.period === month);
  const revs = workbook.lrev.filter((row) => row.partner === partner && row.period === month);
  const revShareSummaries = workbook.lrs.filter((row) => row.partner === partner && row.period === month);
  const fxPartnerPayoutRows = workbook.lfxp.filter((row) => row.partner === partner && row.period === month);
  const revShareRows = workbook.rs.filter((row) => row.partner === partner && inRange(month + "-15", row.startDate, row.endDate));
  const vaData = workbook.lva.find((row) => row.partner === partner && row.period === month);
  const isIncremental = !!workbook.pConfig[partner];
  const periodVolume = txns.reduce((sum, row) => sum + Number(row.totalVolume || 0), 0);
  const minimumRow = workbook.mins.find((row) => row.partner === partner && inRange(month + "-15", row.startDate, row.endDate) && periodVolume >= row.minVol && periodVolume <= row.maxVol);
  const summaryMinimumAmount = revShareSummaries.reduce((max, row) => Math.max(max, Number(row.monthlyMinimumRevenue || 0)), 0);
  const effectiveMinimumAmount = minimumRow && minimumRow.minAmount > 0 ? minimumRow.minAmount : summaryMinimumAmount;
  const fxMarkupActivityRows = txns.filter((row) => (row.txnType === "FX" || (row.payerCcy === "USD" && row.payeeCcy && row.payeeCcy !== "USD")) && row.processingMethod === "Wire");
  const appendLine = function (line) {
    lines.push(Object.assign({ active: true, minimumEligible: false }, line));
  };
  const applyMonthlyMinimumRule = function () {
    if (!(effectiveMinimumAmount > 0)) return;
    const eligibleLines = lines.filter((line) => line.dir === "charge" && line.minimumEligible && line.active !== false);
    const generatedRevenue = eligibleLines.reduce((sum, line) => sum + Number(line.amount || 0), 0);
    const minimumDesc = "Monthly minimum fee for period (" + fmt(effectiveMinimumAmount) + ")";
    if (generatedRevenue < effectiveMinimumAmount) {
      eligibleLines.forEach((line) => {
        line.active = false;
        line.inactiveReason = "Replaced by monthly minimum " + fmt(effectiveMinimumAmount);
      });
      appendLine({ cat: "Minimum", desc: minimumDesc + " replaces " + fmt(generatedRevenue) + " generated revenue", amount: effectiveMinimumAmount, dir: "charge" });
      const implOffset = workbook.impl.find((row) => row.partner === partner && row.applyAgainstMin && String(row.goLiveDate || "").slice(0, 7) === month);
      if (implOffset) {
        appendLine({ cat: "Impl Credit", desc: "vs monthly minimum", amount: Math.min(implOffset.feeAmount, effectiveMinimumAmount), dir: "offset" });
      }
    } else {
      appendLine({ cat: "Minimum", desc: minimumDesc, amount: effectiveMinimumAmount, dir: "charge", active: false, inactiveReason: "Not applicable because generated revenue " + fmt(generatedRevenue) + " exceeds minimum" });
    }
  };

  txns.forEach((txn) => {
    const directInvoiceAmount = Number(txn.directInvoiceAmount || 0);
    if (directInvoiceAmount > 0) {
      const directRate = txn.txnCount > 0 ? directInvoiceAmount / txn.txnCount : Number(txn.directInvoiceRate || 0);
      appendLine({ cat: "Offline", desc: txn.txnType + " " + txn.speedFlag + " " + (txn.processingMethod || "") + " (" + txn.txnCount + "x" + fmt(directRate) + " imported)", amount: directInvoiceAmount, dir: "charge", minimumEligible: true });
      return;
    }
    workbook.off
      .filter((row) => row.partner === partner && optionalMatch(row.txnType, txn.txnType) && optionalMatch(row.speedFlag, txn.speedFlag) && optionalMatch(row.payerFunding, txn.payerFunding) && optionalMatch(row.payeeFunding, txn.payeeFunding) && optionalMatch(row.payerCcy, txn.payerCcy) && optionalMatch(row.payeeCcy, txn.payeeCcy) && optionalMatch(row.payerCountry, txn.payerCountry) && optionalMatch(row.payeeCountry, txn.payeeCountry) && optionalMatch(row.processingMethod, txn.processingMethod) && txn.minAmt >= row.minAmt && txn.maxAmt <= row.maxAmt && inRange(month + "-15", row.startDate, row.endDate))
      .forEach((row) => {
        const amount = row.fee * txn.txnCount;
        appendLine({ cat: "Offline", desc: txn.txnType + " " + txn.speedFlag + " " + (txn.processingMethod || "") + " (" + txn.txnCount + "x" + fmt(row.fee) + ")", amount: amount, dir: "charge", minimumEligible: true });
      });
  });

  txns.forEach((txn) => {
    const allMatching = workbook.vol.filter((row) => row.partner === partner && (!row.txnType || optionalMatch(row.txnType, txn.txnType)) && (!row.speedFlag || optionalMatch(row.speedFlag, txn.speedFlag)) && (!row.payerFunding || optionalMatch(row.payerFunding, txn.payerFunding)) && (!row.payeeFunding || optionalMatch(row.payeeFunding, txn.payeeFunding)) && inRange(month + "-15", row.startDate, row.endDate));
    if (!allMatching.length) return;
    const groups = {};
    allMatching.forEach((row) => {
      const key = [row.txnType, row.speedFlag, row.payerFunding, row.payeeFunding, row.payeeCardType, row.ccyGroup].join("|");
      if (!groups[key]) groups[key] = [];
      groups[key].push(row);
    });
    Object.keys(groups).forEach((key) => {
      const tiers = groups[key].slice().sort((a, b) => a.minVol - b.minVol);
      if (isIncremental && tiers.length > 1) {
        let remaining = txn.totalVolume;
        let totalFee = 0;
        const parts = [];
        tiers.forEach((tier) => {
          if (remaining <= 0) return;
          const bandSize = tier.maxVol - tier.minVol + 1;
          const volumeInBand = Math.min(remaining, bandSize);
          totalFee += tier.rate * volumeInBand;
          parts.push(fmtPct(tier.rate) + "x" + fmt(volumeInBand));
          remaining -= volumeInBand;
        });
        if (totalFee > 0) {
          const adjusted = applyFeeCaps(partner, getProductType(txn, tiers[0]), txn.txnCount > 0 ? totalFee / txn.txnCount : 0, txn.txnCount);
          const amount = adjusted.capped ? adjusted.total : totalFee;
          appendLine({ cat: "Volume", desc: (txn.txnType || "") + " " + (txn.speedFlag || "") + " incremental [" + parts.join(" + ") + "]" + adjusted.capNote, amount: amount, dir: "charge", minimumEligible: true });
        }
      } else {
        tiers.filter((tier) => txn.totalVolume >= tier.minVol && txn.totalVolume <= tier.maxVol).forEach((tier) => {
          const adjusted = applyFeeCaps(partner, getProductType(txn, tier), txn.txnCount > 0 ? (tier.rate * txn.totalVolume) / txn.txnCount : 0, txn.txnCount);
          const amount = adjusted.capped ? adjusted.total : tier.rate * txn.totalVolume;
          appendLine({ cat: "Volume", desc: (txn.txnType || "") + " " + (txn.speedFlag || "") + " " + (tier.note || "") + " (" + fmtPct(tier.rate) + "x" + fmt(txn.totalVolume) + adjusted.capNote + ")", amount: amount, dir: "charge", minimumEligible: true });
        });
      }
    });
  });

  const partnerSurcharges = workbook.surch.filter((row) => row.partner === partner && inRange(month + "-15", row.startDate, row.endDate));
  if (partnerSurcharges.length) {
    const groups = {};
    partnerSurcharges.forEach((row) => {
      if (!groups[row.surchargeType]) groups[row.surchargeType] = [];
      groups[row.surchargeType].push(row);
    });
    txns.forEach((txn) => {
      Object.keys(groups).forEach((type) => {
        const tiers = groups[type].slice().sort((a, b) => a.minVol - b.minVol);
        if (isIncremental && tiers.length > 1) {
          let remaining = txn.totalVolume;
          let totalFee = 0;
          const parts = [];
          tiers.forEach((tier) => {
            if (remaining <= 0) return;
            const bandSize = tier.maxVol - tier.minVol + 1;
            const volumeInBand = Math.min(remaining, bandSize);
            totalFee += tier.rate * volumeInBand;
            parts.push(fmtPct(tier.rate) + "x" + fmt(volumeInBand));
            remaining -= volumeInBand;
          });
          if (totalFee > 0) {
            appendLine({ cat: "Surcharge", desc: type + " incremental [" + parts.join(" + ") + "]", amount: totalFee, dir: "charge", minimumEligible: true });
          }
        } else {
          tiers.filter((tier) => txn.totalVolume >= tier.minVol && txn.totalVolume <= tier.maxVol).forEach((tier) => {
            const amount = tier.rate * txn.totalVolume;
            appendLine({ cat: "Surcharge", desc: tier.surchargeType + " " + (tier.note || "") + " (" + fmtPct(tier.rate) + "x" + fmt(txn.totalVolume) + ")", amount: amount, dir: "charge", minimumEligible: true });
          });
        }
      });
    });
  }

  if (revShareSummaries.length) {
    revShareSummaries.forEach((summary) => {
      if (summary.partnerRevenueShare > 0) {
        appendLine({ cat: "Rev Share", desc: "Partner rev-share payout from revenue report (net revenue " + fmt(summary.netRevenue) + ")", amount: summary.partnerRevenueShare, dir: "pay" });
      } else if (summary.partnerRevenueShare < 0) {
        notes.push("Revenue-share summary for " + partner + " is negative (" + fmt(summary.partnerRevenueShare) + "); no payout line was created.");
      }
      if (summary.revenueOwed > 0) {
        const minNote = summary.monthlyMinimumRevenue > 0 ? ", minimum " + fmt(summary.monthlyMinimumRevenue) : "";
        appendLine({ cat: "Revenue", desc: "Partner-generated revenue from revenue report (" + fmt(summary.revenueOwed) + " owed" + minNote + ")", amount: summary.revenueOwed, dir: "charge", minimumEligible: true });
      }
    });
  } else if (revShareRows.length) {
    const revShareLines = [];
    let revShareMatchCount = 0;
    let revShareRevenueCount = 0;
    revShareRows.forEach((share) => {
      txns.filter((txn) => optionalMatch(share.txnType, txn.txnType) && optionalMatch(share.speedFlag, txn.speedFlag)).forEach((txn) => {
        revShareMatchCount += 1;
        const revenueBasis = txn.revenueBasis || "gross";
        const costRow = revenueBasis === "net" ? null : workbook.pCosts.find((cost) => cost.direction === (txn.txnType === "Payout" ? "Out" : "In") && norm(cost.txnName).includes(norm(txn.processingMethod)));
        const totalCost = costRow ? costRow.fee * txn.txnCount : 0;
        const revenueBase = revenueBasis === "net" ? txn.customerRevenue : txn.customerRevenue - totalCost;
        if (txn.customerRevenue > 0) revShareRevenueCount += 1;
        const payback = share.revSharePct * revenueBase;
        if (payback > 0) {
          const scope = [share.txnType || txn.txnType || "All", share.speedFlag || txn.speedFlag || ""].filter(Boolean).join(" ");
          const desc = revenueBasis === "net"
            ? scope + ": " + fmtPct(share.revSharePct) + "x(" + fmt(txn.customerRevenue) + " net revenue)"
            : scope + ": " + fmtPct(share.revSharePct) + "x(" + fmt(txn.customerRevenue) + " rev-" + fmt(totalCost) + " cost)";
          revShareLines.push({ cat: "Rev Share", desc: desc, amount: payback, dir: "pay" });
        }
      });
    });
    if (!revShareLines.length) {
      const scopes = Array.from(new Set(revShareRows.map((share) => [share.txnType || "All", share.speedFlag || ""].filter(Boolean).join(" ")).filter(Boolean)));
      const scopeLabel = scopes.length ? scopes.join(", ") : "configured rev-share";
      if (!revShareMatchCount) {
        notes.push("Revenue share is configured for " + scopeLabel + ", but no matching transactions were imported for " + partner + " in " + month + ".");
      } else if (!revShareRevenueCount) {
        notes.push("Revenue share is configured for " + scopeLabel + ", but the imported matching transactions have no revenue values. Upload the monthly revenue-share / RTP payout report to calculate the partner payout.");
      }
    }
    revShareLines.forEach((line) => appendLine(line));
  }

  if (fxPartnerPayoutRows.length) {
    fxPartnerPayoutRows.forEach((row) => {
      if (row.partnerPayout > 0) {
        appendLine({
          cat: "Rev Share",
          desc: "FX partner markup payout (" + (row.shareTxnCount || row.txnCount) + " payout txns" + (row.reversalTxnCount ? ", " + row.reversalTxnCount + " reversal txns" : "") + ", markup " + fmt(row.shareAmount || row.partnerPayout) + (row.reversalTxnCount ? " - reversed " + fmt(row.reversalAmount) : "") + ")",
          amount: row.partnerPayout,
          dir: "pay",
        });
      } else if (row.partnerPayout < 0) {
        appendLine({
          cat: "Rev Share",
          desc: "FX partner markup reversal adjustment (" + (row.shareTxnCount || 0) + " payout txns, " + (row.reversalTxnCount || 0) + " reversal txns, net reversed " + fmt(Math.abs(row.partnerPayout)) + ")",
          amount: Math.abs(row.partnerPayout),
          dir: "charge",
        });
      }
      if (row.note) {
        notes.push("Stampli FX payout: " + row.note);
      }
    });
  } else if (partner === "Stampli") {
    if (!fxMarkupActivityRows.length) {
      notes.push("No Stampli FX transactions were imported for " + month + ". The supplied data only contains Domestic and USD Abroad rows, so the FX partner-markup payout remains $0.00.");
    } else {
      notes.push("Stampli FX transactions were imported for " + month + ", but no FX partner-markup payout summary was derived from the raw payment detail.");
    }
  }

  const partnerFxRates = workbook.fxRates.filter((row) => row.partner === partner && inRange(month + "-15", row.startDate, row.endDate));
  if (partnerFxRates.length) {
    txns.filter((txn) => txn.payerCcy !== txn.payeeCcy).forEach((txn) => {
      const avgSize = txn.avgTxnSize || (txn.txnCount > 0 ? txn.totalVolume / txn.txnCount : 0);
      const payeeCorridor = dataScope.getCorridor(txn.payeeCcy);
      const payerCorridor = dataScope.getCorridor(txn.payerCcy);
      const matches = partnerFxRates.filter((row) => {
        const payeeOk = row.payeeCcy ? row.payeeCcy === txn.payeeCcy : (!row.payeeCorridor || row.payeeCorridor === payeeCorridor);
        const payerOk = !row.payerCcy && !row.payerCorridor ? true : (row.payerCcy ? row.payerCcy === txn.payerCcy : row.payerCorridor === payerCorridor);
        const sizeOk = avgSize >= row.minTxnSize && avgSize <= row.maxTxnSize;
        return payeeOk && payerOk && sizeOk;
      });
      if (!matches.length) return;
      const specific = matches.filter((row) => row.payeeCcy === txn.payeeCcy);
      const pool = specific.length ? specific : matches;
      const tiers = pool.slice().sort((a, b) => a.minVol - b.minVol);
      if (isIncremental && tiers.length > 1 && tiers.some((row) => row.minVol !== tiers[0].minVol)) {
        let remaining = txn.totalVolume;
        let totalFee = 0;
        const parts = [];
        tiers.forEach((tier) => {
          if (remaining <= 0) return;
          const bandSize = tier.maxVol - tier.minVol + 1;
          const volumeInBand = Math.min(remaining, bandSize);
          totalFee += tier.rate * volumeInBand;
          parts.push(fmtPct(tier.rate) + "x" + fmt(volumeInBand));
          remaining -= volumeInBand;
        });
        if (totalFee > 0) {
          appendLine({ cat: "FX", desc: txn.payerCcy + "->" + txn.payeeCcy + " incremental [" + parts.join(" + ") + "]", amount: totalFee, dir: "charge", minimumEligible: true });
        }
      } else {
        const best = pool.find((row) => txn.totalVolume >= row.minVol && txn.totalVolume <= row.maxVol) || pool[0];
        if (best) {
          const amount = best.rate * txn.totalVolume;
          appendLine({ cat: "FX", desc: txn.payerCcy + "->" + txn.payeeCcy + " @ " + (best.rate * 100).toFixed(4) + "% (avg txn " + fmt(avgSize) + ") x " + fmt(txn.totalVolume), amount: amount, dir: "charge", minimumEligible: true });
        }
      }
    });
  }

  const partnerReversalFees = workbook.revf.filter((row) => row.partner === partner && inRange(month + "-15", row.startDate, row.endDate));
  revs.forEach((row) => {
    const match = partnerReversalFees.find((fee) => !fee.payerFunding || fee.payerFunding === row.payerFunding);
    if (match) {
      const amount = match.feePerReversal * row.reversalCount;
      appendLine({ cat: "Reversal", desc: (row.payerFunding || "All") + " " + row.reversalCount + "x" + fmt(match.feePerReversal), amount: amount, dir: "charge", minimumEligible: true });
    }
  });

  const platformFee = workbook.plat.find((row) => row.partner === partner && inRange(month + "-15", row.startDate, row.endDate));
  if (platformFee) {
    appendLine({ cat: "Platform", desc: "Monthly subscription", amount: platformFee.monthlyFee, dir: "charge" });
  }

  if (vaData) {
    const partnerVaFees = workbook.vaFees.filter((row) => row.partner === partner);
    if (vaData.newAccountsOpened > 0) {
      const tier = partnerVaFees.filter((row) => row.feeType === "Account Opening").find((row) => vaData.newAccountsOpened >= row.minAccounts && vaData.newAccountsOpened <= row.maxAccounts);
      if (tier) {
        const amount = tier.feePerAccount * vaData.newAccountsOpened;
        appendLine({ cat: "Virtual Acct", desc: "Account Opening: " + vaData.newAccountsOpened + " accts x " + fmt(tier.feePerAccount), amount: amount, dir: "charge", minimumEligible: true });
      }
    }
    if (vaData.totalActiveAccounts > 0) {
      const tier = partnerVaFees.filter((row) => row.feeType === "Monthly Active").find((row) => vaData.totalActiveAccounts >= row.minAccounts && vaData.totalActiveAccounts <= row.maxAccounts);
      if (tier) {
        const amount = tier.feePerAccount * vaData.totalActiveAccounts;
        appendLine({ cat: "Virtual Acct", desc: "Monthly Active: " + vaData.totalActiveAccounts + " accts x " + fmt(tier.feePerAccount) + "/mo", amount: amount, dir: "charge", minimumEligible: true });
      }
    }
    if (vaData.dormantAccounts > 0) {
      const tier = partnerVaFees.filter((row) => row.feeType === "Dormancy").find((row) => vaData.dormantAccounts >= row.minAccounts && vaData.dormantAccounts <= row.maxAccounts);
      if (tier) {
        const amount = tier.feePerAccount * vaData.dormantAccounts;
        appendLine({ cat: "Virtual Acct", desc: "Dormancy: " + vaData.dormantAccounts + " accts x " + fmt(tier.feePerAccount) + "/mo", amount: amount, dir: "charge", minimumEligible: true });
      }
    }
    if (vaData.closedAccounts > 0) {
      const tier = partnerVaFees.filter((row) => row.feeType === "Account Closing").find((row) => vaData.closedAccounts >= row.minAccounts && vaData.closedAccounts <= row.maxAccounts);
      if (tier) {
        const amount = tier.feePerAccount * vaData.closedAccounts;
        appendLine({ cat: "Virtual Acct", desc: "Account Closing: " + vaData.closedAccounts + " accts x " + fmt(tier.feePerAccount), amount: amount, dir: "charge", minimumEligible: true });
      }
    }
    if (vaData.newBusinessSetups > 0) {
      const setupFee = workbook.impl.find((row) => row.partner === partner && row.feeType === "Account Setup");
      if (setupFee) {
        const amount = setupFee.feeAmount * vaData.newBusinessSetups;
        appendLine({ cat: "Account Setup", desc: vaData.newBusinessSetups + " biz x " + fmt(setupFee.feeAmount), amount: amount, dir: "charge", minimumEligible: true });
      }
    }
    if (vaData.settlementCount > 0) {
      const settlementFee = workbook.impl.find((row) => row.partner === partner && row.feeType === "Daily Settlement");
      if (settlementFee) {
        const amount = settlementFee.feeAmount * vaData.settlementCount;
        appendLine({ cat: "Settlement", desc: vaData.settlementCount + " sweeps x " + fmt(settlementFee.feeAmount), amount: amount, dir: "charge", minimumEligible: true });
      }
    }
  }
  applyMonthlyMinimumRule();

  const implFee = workbook.impl.find((row) => row.partner === partner && String(row.goLiveDate || "").slice(0, 7) === month && !row.applyAgainstMin);
  if (implFee) {
    appendLine({ cat: "Impl Fee", desc: "Go-live fee", amount: implFee.feeAmount, dir: "charge" });
  }

  const totals = calculateActiveInvoiceTotals(lines);
  const chargeTotal = totals.chg;
  const payTotal = totals.pay;

  return {
    partner: partner,
    period: month,
    lines: lines,
    notes: notes,
    chg: chargeTotal,
    pay: payTotal,
    net: chargeTotal - payTotal,
    dir: chargeTotal - payTotal >= 0 ? "Partner Owes Us" : "We Owe Partner"
  };
}

function joinUnique(values) {
  const seen = {};
  const ordered = [];
  values.forEach((value) => {
    if (!value || seen[value]) return;
    seen[value] = true;
    ordered.push(value);
  });
  return ordered;
}

const activePartners = joinUnique(
  workbook.ltxn.concat(workbook.lrev, workbook.lrs, workbook.lva)
    .filter((row) => row && row.period === period)
    .map((row) => row.partner)
).sort();

const invoices = activePartners.map((partner) => calculateInvoice(partner, period));
invoices.sort((a, b) => Math.abs(b.net) - Math.abs(a.net));

const manualInputs = [];
if (importBundle.meta && importBundle.meta.offline && importBundle.meta.offline.unmatchedPaymentIds) {
  manualInputs.push({
    item: "Unmatched fixed-billing partner mapping",
    detail: importBundle.meta.offline.unmatchedPaymentIds + " payment IDs still need a partner mapping before offline invoice totals are complete."
  });
}
if (workbook.vaFees.some((row) => row.partner === "Blindpay") && !workbook.lva.some((row) => row.partner === "Blindpay" && row.period === period && (row.newAccountsOpened || row.dormantAccounts || row.totalActiveAccounts))) {
  manualInputs.push({
    item: "Blindpay virtual account counts",
    detail: "Blindpay account openings and dormancy still need validation if the registered-accounts export is incomplete for this month."
  });
}
if (workbook.impl.some((row) => row.partner === "Nuvion" && row.feeType === "Daily Settlement")) {
  manualInputs.push({
    item: "Nuvion settlement validation",
    detail: "Settlement sweeps are currently derived as one settlement count per transaction day. If Nuvion can sweep multiple times per day, an explicit settlement export is still needed."
  });
}
if (workbook.vaFees.some((row) => row.partner === "Yeepay" && row.feeType === "Account Closing")) {
  manualInputs.push({
    item: "Yeepay account-closing counts",
    detail: "Need " + period + " count of inactive virtual accounts that were actually closed so the $5 account-closing fee can be billed if applicable."
  });
}
const revSharePartnersWithoutMin = [...new Set(workbook.rs.map((row) => row.partner))].filter((partner) => !workbook.mins.some((row) => row.partner === partner));
if (revSharePartnersWithoutMin.length) {
  manualInputs.push({
    item: "Rev-share monthly minimum config",
    detail: "If any rev-share partner has a contractual monthly minimum, it still needs to be entered in the workbook Min Rev table for: " + revSharePartnersWithoutMin.join(", ") + "."
  });
}

const report = [
  "# Invoice Calculations",
  "",
  "- Period: `" + period + "`",
  "- Partners with imported activity or summary data: `" + activePartners.length + "`",
  "",
  "## Invoice Totals",
  ""
];

invoices.forEach((invoice) => {
  report.push("- `" + invoice.partner + "`: " + invoice.dir + " " + fmt(Math.abs(invoice.net)) + " (charges " + fmt(invoice.chg) + ", pays " + fmt(invoice.pay) + ", lines " + invoice.lines.length + ")");
});

report.push("", "## Manual Inputs Still Needed", "");
manualInputs.forEach((item) => {
  report.push("- `" + item.item + "`: " + item.detail);
});

report.push("", "## Partner Line Detail", "");
invoices.forEach((invoice) => {
  report.push("### " + invoice.partner, "");
  if (!invoice.lines.length) {
    report.push("- No billable lines from the imported data.");
  } else {
    invoice.lines.forEach((line) => {
      const direction = line.dir === "pay" ? "pay" : line.dir === "offset" ? "offset" : "charge";
      const inactive = line.active === false ? " [inactive]" : "";
      const reason = line.active === false && line.inactiveReason ? " (" + line.inactiveReason + ")" : "";
      report.push("- [" + direction + inactive + "] `" + line.cat + "` " + line.desc + " = " + fmt(line.amount) + reason);
    });
  }
  invoice.notes.forEach((note) => {
    report.push("- Note: " + note);
  });
  report.push("- Net: `" + invoice.dir + " " + fmt(Math.abs(invoice.net)) + "`", "");
});

const jsonPath = outputDir + "/invoice_report.json";
const mdPath = outputDir + "/invoice_report.md";
writeText(jsonPath, JSON.stringify({ period: period, invoices: invoices, manualInputs: manualInputs }, null, 2) + "\n");
writeText(mdPath, report.join("\n") + "\n");
console.log("Wrote invoice JSON to " + jsonPath);
console.log("Wrote invoice report to " + mdPath);
