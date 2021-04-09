import { IClientSubscribeOptions } from "mqtt";

type MqttSubscribeOptions = IClientSubscribeOptions & {

	receiveAllTopics?: boolean;

};

export default MqttSubscribeOptions;
