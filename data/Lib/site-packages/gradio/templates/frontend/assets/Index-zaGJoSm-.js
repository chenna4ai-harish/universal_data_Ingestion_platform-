import { r as rest_props } from './i18n-CGlVUOvE.js';
import { M as push, ad as user_effect, O as pop } from './index-Bq2njFOY.js';
import { G as Gradio } from './utils.svelte-Bi9ASwv9.js';

function Index($$anchor, $$props) {
	push($$props, true);

	let props = rest_props($$props, ['$$slots', '$$events', '$$legacy']);
	const gradio = new Gradio(props);

	user_effect(() => {
		gradio.props.value && gradio.dispatch("change");
	});

	pop();
}

export { Index as default };
//# sourceMappingURL=Index-zaGJoSm-.js.map
