import MqttPacket from "./MqttPacket";

export default interface MqttMessage {

	readonly topic: string;

	readonly payload: Buffer;

	readonly packet: MqttPacket;

}
