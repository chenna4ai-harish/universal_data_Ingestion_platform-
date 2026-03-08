import { p as prop, b as set_class } from './i18n-CGlVUOvE.js';
import { t as template_effect, X as set_text, a as append, R as from_html, Q as child, T as reset } from './index-Bq2njFOY.js';
/* empty css                                               */

var root = from_html(`<pre> </pre>`);

function Example($$anchor, $$props) {

	let selected = prop($$props, 'selected', 3, false);

	function truncate_text(text, max_length = 60) {
		if (!text) return "";

		const str = String(text);

		if (str.length <= max_length) return str;

		return str.slice(0, max_length) + "...";
	}

	var pre = root();
	let classes;
	var text_1 = child(pre, true);

	reset(pre);

	template_effect(
		($0) => {
			classes = set_class(pre, 1, 'svelte-1bbj91m', null, classes, {
				table: $$props.type === "table",
				gallery: $$props.type === "gallery",
				selected: selected()
			});

			set_text(text_1, $0);
		},
		[() => truncate_text($$props.value)]
	);

	append($$anchor, pre);
}

export { Example as default };
//# sourceMappingURL=Example-BYnu_U1h.js.map
