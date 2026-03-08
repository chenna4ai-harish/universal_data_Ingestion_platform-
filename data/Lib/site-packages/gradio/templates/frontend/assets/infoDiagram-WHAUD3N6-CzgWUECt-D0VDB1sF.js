import { _ as __name, l as log, H as selectSvgElement, e as configureSvgSize, I as package_default } from './mermaid.core-vMMZVCDT-alxhRXoZ.js';
import { p as parse } from './treemap-KMMF4GRG-BLLAjc28-BCyzwAS3.js';
import './index-Bq2njFOY.js';
import './Index-BOG-LPoW.js';
import './i18n-CGlVUOvE.js';
import './utils.svelte-Bi9ASwv9.js';
import './index-DnoGeqVF.js';
import './dsv-BhAd467f.js';
import './props-o1aFOJaB.js';
import './misc-D8a4ZbMA.js';
import './index-By61_kAe.js';
import './Upload-xR3P-2U5.js';
import './snippet-BG7qkY_1.js';
import './actions-CgRQ2lHA.js';
import './ScrollFade.svelte_svelte_type_style_lang-D0JPkGar.js';
import './MarkdownCode.svelte_svelte_type_style_lang-B29zDiob.js';
import './prism-python-NrKIQnfs.js';
import './html-Bwif0JPw.js';
import './input-CXpCw23l.js';
import './event-modifiers-DanhKw3_.js';
import './MarkdownCode-BYKuypS7.js';
import './StreamingBar.svelte_svelte_type_style_lang-DWGn5tT_.js';
import './Checkbox-dsIC6373.js';
import './size-CWi277d_.js';
import './Check-BKMHx_DF.js';
import './DropdownArrow-3lFHYtTD.js';
import './Copy-BQlJe6-D.js';
import './FullscreenButton-C6RgeACK.js';
import './Maximize-2N5airbC.js';
import './Example-CSyNUPmz.js';
import './min-BP3TZd4l-Co7ym5UQ.js';
import './_baseUniq-CrYdfo_J-f6Jo4gbc.js';

var parser = {
  parse: /* @__PURE__ */ __name(async (input) => {
    const ast = await parse("info", input);
    log.debug(ast);
  }, "parse")
};
var DEFAULT_INFO_DB = {
  version: package_default.version + ""
};
var getVersion = /* @__PURE__ */ __name(() => DEFAULT_INFO_DB.version, "getVersion");
var db = {
  getVersion
};
var draw = /* @__PURE__ */ __name((text, id, version) => {
  log.debug("rendering info diagram\n" + text);
  const svg = selectSvgElement(id);
  configureSvgSize(svg, 100, 400, true);
  const group = svg.append("g");
  group.append("text").attr("x", 100).attr("y", 40).attr("class", "version").attr("font-size", 32).style("text-anchor", "middle").text(`v${version}`);
}, "draw");
var renderer = { draw };
var diagram = {
  parser,
  db,
  renderer
};

export { diagram };
//# sourceMappingURL=infoDiagram-WHAUD3N6-CzgWUECt-D0VDB1sF.js.map
