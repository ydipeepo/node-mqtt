import { connect, MqttClient as MqttClientCore } from "mqtt";
import { AsyncStream, ConcurrentQueue, Signal } from "@ydipeepo/node-async";
import MqttClientOptions from "./MqttClientOptions";
import MqttPublishOptions from "./MqttPublishOptions";
import MqttSubscribeOptions from "./MqttSubscribeOptions";
import MqttPacket from "./MqttPacket";
import MqttMessage from "./MqttMessage";

interface MqttClient {

	readonly connected: boolean;

	readonly reconnecting: boolean;

	publish(topic: string | string[], message: string, options?: MqttPublishOptions): Promise<MqttPacket>;

	subscribe(topic: string | string[], options?: MqttSubscribeOptions): AsyncStream<MqttMessage>;

	close(force?: boolean): Promise<void>;

}

function match(patternArray: string[], receiveArray: string[]) {
	for (let i = 0; i < patternArray.length; ++i) {
		if (i >= receiveArray.length) {
			return false;
		}
		if (patternArray[i] === '#') {
			return true;
		}
		if (patternArray[i] !== '+' && patternArray[i] != receiveArray[i]) {
			return false;
		}
	}
	return patternArray.length === receiveArray.length;
}

class MqttClientImpl implements MqttClient {

	private readonly stopRequest = new Signal();

	private async *subscribeGenerator(topics: string[], options?: MqttSubscribeOptions): AsyncGenerator<MqttMessage, void, void> {

		const messageQueue = new ConcurrentQueue<MqttMessage>();

		const patternArrays = topics.map(topic => topic.split('/'));

		const handleMessage = options?.receiveAllTopics ?? false
			? (receiveTopic: string, payload: Buffer, packet: MqttPacket) => {
				const receiveArray = receiveTopic.split('/');
				for (const patternArray of patternArrays) {
					if (match(patternArray, receiveArray)) {
						messageQueue.add({
							topic: receiveTopic,
							payload,
							packet,
						});
						return;
					}
				}
			}
			: (receiveTopic: string, payload: Buffer, packet: MqttPacket) => messageQueue.add({
				topic: receiveTopic,
				payload,
				packet,
			});

		await new Promise<void>((resolve, reject) => void this.client.subscribe(topics, options, error => {
			if (error) {
				reject(error);
			} else {
				this.client.on("message", handleMessage);
				resolve();
			}
		}));

		const messageStream = messageQueue.getMultiple(this.stopRequest);
		try {
			for await (const message of messageStream) {
				yield message;
			}
		} finally {
			await messageStream.return();
			await new Promise<void>((resolve, reject) => void this.client.unsubscribe(topics, undefined, error => {
				this.client.off("message", handleMessage);
				if (error) {
					reject(error);
				} else {
					resolve();
				}
			}));
		}

	}

	get connected() {
		return this.client.connected;
	}

	get reconnecting() {
		return this.client.reconnecting;
	}

	constructor(private readonly client: MqttClientCore) {
	}

	publish(topic: string, message: string | Buffer, options?: MqttPublishOptions) {
		return new Promise<MqttPacket>((resolve, reject) => {
			this.client.publish(topic, message, options, (error, packet) => {
				if (error) {
					reject(error);
				} else {
					resolve(packet);
				}
			});
		});
	}

	subscribe(topic: string | string[], options?: MqttSubscribeOptions) {
		if (typeof topic === "string") {
			topic = [topic];
		}
		return AsyncStream.from(this.subscribeGenerator(topic, options));
	}

	close(force?: boolean) {
		this.stopRequest.trigger();
		return new Promise<void>(resolve => void this.client.end(force, undefined, resolve));
	}

}

namespace MqttClient {

	export async function create(brokerUrl: string, options?: MqttClientOptions): Promise<MqttClient> {
		const mqttClient = connect(brokerUrl, options);
		await new Promise<void>(resolve => mqttClient.on("connect", resolve));
		return new MqttClientImpl(mqttClient);
	}

}

export default MqttClient;
