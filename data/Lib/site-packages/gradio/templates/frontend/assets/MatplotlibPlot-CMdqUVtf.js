import { a as set_attribute } from './i18n-CGlVUOvE.js';
import { M as push, t as template_effect, a as append, O as pop, R as from_html, Z as get, a2 as user_derived, Q as child, T as reset } from './index-Bq2njFOY.js';

var root = from_html(`<div class="matplotlib layout svelte-n8pych"><img class="svelte-n8pych"/></div>`);

function MatplotlibPlot($$anchor, $$props) {
	push($$props, true);

	let plot = user_derived(() => $$props.value?.plot);
	var div = root();

	set_attribute(div, 'data-testid', "matplotlib");

	var img = child(div);

	reset(div);

	template_effect(() => {
		set_attribute(img, 'src', get(plot));
		set_attribute(img, 'alt', `${$$props.value.chart} plot visualising provided data`);
	});

	append($$anchor, div);
	pop();
}

export { MatplotlibPlot as default };
//# sourceMappingURL=MatplotlibPlot-CMdqUVtf.js.map
