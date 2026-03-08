import { p as prop, e as init, i as if_block, b as set_class } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, a as append, O as pop, P as flushSync, R as from_html, Q as child, y as deep_read_state, w as untrack, T as reset } from './index-Bq2njFOY.js';
import { I as Image } from './Image-XuMqTTwB.js';
/* empty css                                               */
import './misc-D8a4ZbMA.js';
/* empty css                                             */

var root = from_html(`<div><!></div>`);

function Example($$anchor, $$props) {
	push($$props, false);

	let value = prop($$props, 'value', 12);
	let type = prop($$props, 'type', 12);
	let selected = prop($$props, 'selected', 12, false);

	var $$exports = {
		get value() {
			return value();
		},

		set value($$value) {
			value($$value);
			flushSync();
		},

		get type() {
			return type();
		},

		set type($$value) {
			type($$value);
			flushSync();
		},

		get selected() {
			return selected();
		},

		set selected($$value) {
			selected($$value);
			flushSync();
		}
	};

	init();

	var div = root();
	let classes;
	var node = child(div);

	{
		var consequent = ($$anchor) => {
			Image($$anchor, {
				get src() {
					return (deep_read_state(value()), untrack(() => value().url));
				},
				alt: ''
			});
		};

		if_block(node, ($$render) => {
			if (value()) $$render(consequent);
		});
	}

	reset(div);

	template_effect(() => classes = set_class(div, 1, 'container svelte-bs74gu', null, classes, {
		table: type() === "table",
		gallery: type() === "gallery",
		selected: selected(),
		border: value()
	}));

	append($$anchor, div);

	return pop($$exports);
}

export { Example as default };
//# sourceMappingURL=Example-4MLRTjBC.js.map
