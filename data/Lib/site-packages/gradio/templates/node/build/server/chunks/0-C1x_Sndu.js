import { r as redirect } from './index-wpIsICWW.js';
import { d as dev } from './index5-BZVOFaHm.js';
import './dev-fallback-B-RpELjM.js';

function load({ url }) {
  const { pathname, search } = url;
  if (dev && url.pathname.startsWith("/theme")) {
    redirect(308, `http://127.0.0.1:7860${pathname}${search}`);
  }
}

var _layout_server_ts = /*#__PURE__*/Object.freeze({
  __proto__: null,
  load: load
});

const index = 0;
let component_cache;
const component = async () => component_cache ??= (await import('./_layout.svelte-q8pmoRpx.js')).default;
const server_id = "src/routes/+layout.server.ts";
const imports = ["_app/immutable/nodes/0.DNIORK8s.js","_app/immutable/chunks/Bwz6a6nB.js","_app/immutable/chunks/D_0idP7x.js","_app/immutable/chunks/BHyOYhsu.js","_app/immutable/chunks/CcnjDNIR.js"];
const stylesheets = ["_app/immutable/assets/0.B8qBVJRr.css"];
const fonts = [];

export { component, fonts, imports, index, _layout_server_ts as server, server_id, stylesheets };
//# sourceMappingURL=0-C1x_Sndu.js.map
