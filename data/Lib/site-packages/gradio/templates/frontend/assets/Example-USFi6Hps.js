import { p as prop, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, X as set_text, a as append, O as pop, R as from_html, Q as child, T as reset } from './index-Bq2njFOY.js';
/* empty css                                               */

var root = from_html(`<div> </div>`);

function Example($$anchor, $$props) {
	push($$props, true);

	let selected = prop($$props, 'selected', 3, false);
	var div = root();
	let classes;
	var text = child(div, true);

	reset(div);

	template_effect(
		($0) => {
			classes = set_class(div, 1, 'svelte-1p04unr', null, classes, {
				table: $$props.type === "table",
				gallery: $$props.type === "gallery",
				selected: selected()
			});

			set_text(text, $0);
		},
		[
			() => $$props.value
				? Array.isArray($$props.value) ? $$props.value.join(", ") : $$props.value
				: ""
		]
	);

	append($$anchor, div);
	pop();
}

export { Example as default };
//# sourceMappingURL=Example-USFi6Hps.js.map
