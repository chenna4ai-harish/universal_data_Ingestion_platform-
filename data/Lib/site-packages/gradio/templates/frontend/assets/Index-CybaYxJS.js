import { r as rest_props } from './i18n-CGlVUOvE.js';
import { M as push, ad as user_effect, af as onDestroy, O as pop } from './index-Bq2njFOY.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';

function Index($$anchor, $$props) {
	push($$props, true);

	const props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(props);
	let interval = undefined;

	user_effect(() => {
		if (interval) clearInterval(interval);

		if (gradio.props.active) {
			interval = setInterval(
				() => {
					if (document.visibilityState === "visible") {
						gradio.dispatch("tick");
					}
				},
				gradio.props.value * 1000
			);
		}
	});

	onDestroy(() => {
		if (interval) clearInterval(interval);
	});

	pop();
}

export { Index as default };
//# sourceMappingURL=Index-CybaYxJS.js.map
