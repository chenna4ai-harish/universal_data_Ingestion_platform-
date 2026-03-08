const manifest = (() => {
function __memo(fn) {
	let value;
	return () => value ??= (value = fn());
}

return {
	appDir: "_app",
	appPath: "_app",
	assets: new Set([]),
	mimeTypes: {},
	_: {
		client: {start:"_app/immutable/entry/start.CdhvqEK0.js",app:"_app/immutable/entry/app.BF69EddK.js",imports:["_app/immutable/entry/start.CdhvqEK0.js","_app/immutable/chunks/Dn6kO8JW.js","_app/immutable/chunks/D_0idP7x.js","_app/immutable/chunks/C0L-Xv56.js","_app/immutable/entry/app.BF69EddK.js","_app/immutable/chunks/DQl9thHM.js","_app/immutable/chunks/D_0idP7x.js","_app/immutable/chunks/C0L-Xv56.js","_app/immutable/chunks/Cg5A2mhh.js","_app/immutable/chunks/Bwz6a6nB.js"],stylesheets:[],fonts:[],uses_env_dynamic_public:false},
		nodes: [
			__memo(() => import('./chunks/0-C1x_Sndu.js')),
			__memo(() => import('./chunks/1-CFR9voR5.js')),
			__memo(() => import('./chunks/2-BYQShRaz.js').then(function (n) { return n._; }))
		],
		remotes: {
			
		},
		routes: [
			{
				id: "/[...catchall]",
				pattern: /^(?:\/([^]*))?\/?$/,
				params: [{"name":"catchall","optional":false,"rest":true,"chained":true}],
				page: { layouts: [0,], errors: [1,], leaf: 2 },
				endpoint: null
			}
		],
		prerendered_routes: new Set([]),
		matchers: async () => {
			
			return {  };
		},
		server_assets: {}
	}
}
})();

const prerendered = new Set([]);

const base = "";

export { base, manifest, prerendered };
//# sourceMappingURL=manifest.js.map
