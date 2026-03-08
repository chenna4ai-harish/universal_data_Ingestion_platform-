import { p as prop, b as set_class } from './i18n-CGlVUOvE.js';
import { t as template_effect, a as append, R as from_html, X as set_text, Q as child, T as reset } from './index-Bq2njFOY.js';

var root = from_html(`<div> </div>`);

function Example($$anchor, $$props) {

	let selected = prop($$props, 'selected', 3, false);
	var div = root();
	let classes;
	var text = child(div, true);

	reset(div);

	template_effect(() => {
		classes = set_class(div, 1, 'svelte-9pg6fh', null, classes, {
			table: $$props.type === "table",
			gallery: $$props.type === "gallery",
			selected: selected()
		});

		set_text(text, $$props.value ? $$props.value : "");
	});

	append($$anchor, div);
}

export { Example as default };
//# sourceMappingURL=Example-D-PFeR7G.js.map
