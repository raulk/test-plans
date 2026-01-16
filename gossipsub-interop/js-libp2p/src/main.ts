// Polyfill CustomEvent for Shadow network simulator environment
if (typeof globalThis.CustomEvent === 'undefined') {
  // @ts-ignore - Custom polyfill for environments without CustomEvent
  globalThis.CustomEvent = class CustomEvent<T = unknown> extends Event {
    detail: T;
    constructor(type: string, eventInitDict?: CustomEventInit<T>) {
      super(type, eventInitDict);
      this.detail = eventInitDict?.detail as T;
    }
  };
}

// Polyfill Promise.withResolvers for older Node.js versions
if (!('withResolvers' in Promise)) {
  // @ts-ignore - Custom polyfill for environments without Promise.withResolvers
  (Promise as unknown as { withResolvers: unknown }).withResolvers = function<T>(): { promise: Promise<T>; resolve: (value: T) => void; reject: (reason?: unknown) => void } {
    let resolve!: (value: T) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new Promise<T>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    return { promise, resolve, reject };
  };
}

import { readFileSync } from 'node:fs';
import { hostname } from 'node:os';
import { createLibp2p, type Libp2p } from 'libp2p';
import { tcp } from '@libp2p/tcp';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { identify } from '@libp2p/identify';
import { gossipsub, type GossipSub } from '@chainsafe/libp2p-gossipsub';
import { generateKeyPairFromSeed } from '@libp2p/crypto/keys';
import { peerIdFromPrivateKey } from '@libp2p/peer-id';
import { multiaddr } from '@multiformats/multiaddr';
import { fromString as uint8ArrayFromString } from 'uint8arrays/from-string';
import { toString as uint8ArrayToString } from 'uint8arrays/to-string';
import type { PeerId, PrivateKey, Message } from '@libp2p/interface';

// Types for script instructions
interface GossipSubParams {
  D?: number;
  Dlo?: number;
  Dhi?: number;
  Dscore?: number;
  Dout?: number;
  Dlazy?: number;
  GossipFactor?: number;
  HeartbeatInitialDelay?: number;
  HeartbeatInterval?: number;
}

interface Connect {
  type: 'connect';
  connectTo: number[];
}

interface IfNodeIDEquals {
  type: 'ifNodeIDEquals';
  nodeID: number;
  instruction: ScriptInstruction;
}

interface WaitUntil {
  type: 'waitUntil';
  elapsedSeconds: number;
}

interface AddPartialMessage {
  type: 'addPartialMessage';
  parts: number;
  topicID: string;
  groupID: number;
}

interface PublishPartial {
  type: 'publishPartial';
  topicID: string;
  groupID: number;
  publishToNodeIDs?: number[] | null;
}

interface Publish {
  type: 'publish';
  messageID: number;
  messageSizeBytes: number;
  topicID: string;
}

interface SubscribeToTopic {
  type: 'subscribeToTopic';
  topicID: string;
  partial?: boolean;
}

interface SetTopicValidationDelay {
  type: 'setTopicValidationDelay';
  topicID: string;
  delaySeconds: number;
}

interface InitGossipSub {
  type: 'initGossipSub';
  gossipSubParams: GossipSubParams;
}

type ScriptInstruction =
  | Connect
  | IfNodeIDEquals
  | WaitUntil
  | AddPartialMessage
  | PublishPartial
  | Publish
  | SubscribeToTopic
  | SetTopicValidationDelay
  | InitGossipSub;

interface ExperimentParams {
  script: ScriptInstruction[];
}

// Global state
let nodeId: number = 0;
let peerId: PeerId;
let privKey: PrivateKey;
let libp2pHost: Libp2p | undefined;
let gossipsubRouter: GossipSub | undefined;
const partialMessages = new Map<bigint, InteropPartialMessage>();
const startTime = Date.now();
console.error(`Module loaded, startTime=${startTime}`);
const subscribedTopics = new Set<string>();

// Partial message implementation for interop
// Each part is 1024 bytes, matching the Go implementation
const PART_LEN = 1024;
const NUM_PARTS = 8;

class InteropPartialMessage {
  private readonly groupIdBytes: Uint8Array;
  private partsBitmap: number = 0; // 8-bit bitmap of which parts we have
  private partsData: (Uint8Array | null)[] = new Array(NUM_PARTS).fill(null);

  constructor(groupIdBytes: Uint8Array) {
    this.groupIdBytes = groupIdBytes;
  }

  // Fill parts with deterministic data, matching the Go algorithm
  fillParts(partsBitmap: number): void {
    const view = new DataView(this.groupIdBytes.buffer, this.groupIdBytes.byteOffset, 8);
    const startCounter = view.getBigUint64(0, false); // big-endian

    for (let i = 0; i < NUM_PARTS; i++) {
      if ((partsBitmap & (1 << i)) === 0) {
        continue;
      }
      if (this.partsData[i] !== null) {
        continue; // Already have this part
      }

      // Fill part with deterministic data
      const partData = new Uint8Array(PART_LEN);
      const partView = new DataView(partData.buffer);
      let counter = startCounter + BigInt(i * PART_LEN / 8);

      for (let j = 0; j < PART_LEN / 8; j++) {
        partView.setBigUint64(j * 8, counter, false); // big-endian
        counter++;
      }

      this.partsData[i] = partData;
      this.partsBitmap |= (1 << i);
    }
  }

  extend(data: Uint8Array): boolean {
    // Data format: [partsBitmap (1 byte)] [part data...] [groupId (8 bytes)]
    if (data.length < 9) return false;

    const receivedBitmap = data[0];
    const groupIdStart = data.length - 8;
    const payloadData = data.slice(1, groupIdStart);

    // Verify groupId matches
    const receivedGroupId = data.slice(groupIdStart);
    for (let i = 0; i < 8; i++) {
      if (receivedGroupId[i] !== this.groupIdBytes[i]) {
        console.error(`extend: groupId mismatch at byte ${i}`);
        return false;
      }
    }

    // Parse parts from the payload
    let payloadOffset = 0;
    for (let i = 0; i < NUM_PARTS; i++) {
      if (payloadOffset >= payloadData.length) {
        break;
      }
      if ((receivedBitmap & (1 << i)) === 0) {
        // This part is not in the message
        continue;
      }
      if (this.partsData[i] !== null) {
        // Already have this part, skip the data
        payloadOffset += PART_LEN;
        continue;
      }
      if (payloadOffset + PART_LEN > payloadData.length) {
        console.error(`extend: not enough data for part ${i}`);
        break;
      }

      // Copy this part
      this.partsData[i] = payloadData.slice(payloadOffset, payloadOffset + PART_LEN);
      this.partsBitmap |= (1 << i);
      payloadOffset += PART_LEN;
    }

    return this.isComplete();
  }

  isComplete(): boolean {
    return this.partsBitmap === 0xff;
  }

  partsMetadata(): Uint8Array {
    return new Uint8Array([this.partsBitmap]);
  }

  groupId(): Uint8Array {
    return this.groupIdBytes;
  }

  partialMessageBytes(requestedMeta: Uint8Array | null): {
    needMore: boolean;
    bytesToSend: Uint8Array | null;
    updatedPartsMetadata: Uint8Array;
  } {
    const ourMeta = this.partsMetadata();

    // Determine which parts to send (parts we have that they don't)
    const theirParts = requestedMeta?.[0] ?? 0;
    const partsToSend = this.partsBitmap & ~theirParts;

    if (partsToSend === 0) {
      return {
        needMore: !this.isComplete(),
        bytesToSend: null,
        updatedPartsMetadata: ourMeta,
      };
    }

    // Count how many parts we're sending
    let numPartsToSend = 0;
    for (let i = 0; i < NUM_PARTS; i++) {
      if ((partsToSend & (1 << i)) !== 0 && this.partsData[i] !== null) {
        numPartsToSend++;
      }
    }

    // Create message: [parts bitmap (1 byte)] [part data...] [groupId (8 bytes)]
    const payloadSize = 1 + numPartsToSend * PART_LEN + 8;
    const payload = new Uint8Array(payloadSize);
    let actualBitmapSent = 0;
    let offset = 1;

    for (let i = 0; i < NUM_PARTS; i++) {
      if ((partsToSend & (1 << i)) === 0) {
        continue;
      }
      const partData = this.partsData[i];
      if (partData === null) {
        continue;
      }
      payload.set(partData, offset);
      offset += PART_LEN;
      actualBitmapSent |= (1 << i);
    }

    payload[0] = actualBitmapSent;
    payload.set(this.groupIdBytes, offset);

    // Merge metadata
    const mergedMeta = new Uint8Array([theirParts | this.partsBitmap]);

    return {
      needMore: mergedMeta[0] !== 0xff,
      bytesToSend: payload,
      updatedPartsMetadata: mergedMeta,
    };
  }
}

function logJson(msg: string, extra: Record<string, unknown> = {}): void {
  const logEntry = {
    time: new Date().toISOString(),
    level: 'INFO',
    msg,
    service: 'gossipsub',
    ...extra,
  };
  console.log(JSON.stringify(logEntry));
}

// Generate deterministic peer ID from node ID
async function generateDeterministicKey(nodeId: number): Promise<PrivateKey> {
  const seed = new Uint8Array(32);
  // Little-endian encode the node ID
  const view = new DataView(seed.buffer);
  view.setUint32(0, nodeId, true);
  return generateKeyPairFromSeed('Ed25519', seed);
}

// Get peer ID for a node ID
async function getPeerIdForNode(targetNodeId: number): Promise<PeerId> {
  const key = await generateDeterministicKey(targetNodeId);
  return peerIdFromPrivateKey(key);
}

async function initGossipSub(params: GossipSubParams): Promise<void> {
  libp2pHost = await createLibp2p({
    privateKey: privKey,
    addresses: {
      listen: ['/ip4/0.0.0.0/tcp/9000'],
    },
    transports: [tcp({
      // Set very large inactivity timeout (10 minutes) instead of 0
      // to prevent idle connections from being closed
      inboundSocketInactivityTimeout: 600000, // 10 minutes
      outboundSocketInactivityTimeout: 600000, // 10 minutes
      // Close timeout also extended
      socketCloseTimeout: 60000, // 1 minute
    })],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux({
      // Enable yamux keepalive to prevent stream timeouts
      enableKeepAlive: true,
      keepAliveInterval: 5000, // 5 seconds
    })],
    connectionManager: {
      // Increase timeouts to prevent premature connection closure in Shadow
      // Default is 10 seconds which causes connections to close during idle periods
      inboundUpgradeTimeout: 120000, // 2 minutes
      outboundUpgradeTimeout: 120000, // 2 minutes
      inboundStreamProtocolNegotiationTimeout: 120000, // 2 minutes
      outboundStreamProtocolNegotiationTimeout: 120000, // 2 minutes
    },
    // Disable the ConnectionMonitor which sends pings every 10 seconds and aborts
    // connections on ping failure. We rely on yamux keepalive instead.
    connectionMonitor: {
      enabled: false,
    },
    services: {
      identify: identify(),
      pubsub: gossipsub({
        D: params.D ?? 6,
        Dlo: params.Dlo ?? 4,
        Dhi: params.Dhi ?? 12,
        doPX: false,
        allowPublishToZeroTopicPeers: true,
        // Enable partial messages
        partialMessages: true,
        partsMetadataMerger: {
          merge(a: Uint8Array, b: Uint8Array): Uint8Array {
            const result = new Uint8Array(Math.max(a.length, b.length));
            for (let i = 0; i < result.length; i++) {
              result[i] = (a[i] ?? 0) | (b[i] ?? 0);
            }
            return result;
          },
          hasAllParts(metadata: Uint8Array, totalParts: number): boolean {
            let count = 0;
            for (const byte of metadata) {
              for (let bit = 0; bit < 8; bit++) {
                if ((byte & (1 << bit)) !== 0) count++;
              }
            }
            return count >= totalParts;
          },
          countParts(metadata: Uint8Array): number {
            let count = 0;
            for (const byte of metadata) {
              for (let bit = 0; bit < 8; bit++) {
                if ((byte & (1 << bit)) !== 0) count++;
              }
            }
            return count;
          },
          getSetBits(metadata: Uint8Array): number[] {
            const bits: number[] = [];
            for (let byteIdx = 0; byteIdx < metadata.length; byteIdx++) {
              for (let bit = 0; bit < 8; bit++) {
                if ((metadata[byteIdx] & (1 << bit)) !== 0) {
                  bits.push(byteIdx * 8 + bit);
                }
              }
            }
            return bits;
          },
        },
        partialMessageExtension: {
          onIncomingRpc(
            peerId: PeerId,
            topicID: Uint8Array,
            groupID: Uint8Array,
            partsMetadata: Uint8Array | undefined,
            partialMessage: Uint8Array | undefined
          ): void {
            handlePartialRpc(peerId, topicID, groupID, partsMetadata, partialMessage);
          },
        },
      }),
    },
  });

  await libp2pHost.start();

  gossipsubRouter = libp2pHost.services.pubsub as GossipSub;

  // Add connection event listeners for debugging
  libp2pHost.addEventListener('connection:close', (evt) => {
    console.error(`connection:close event - remotePeer=${evt.detail.remotePeer.toString().slice(0, 16)}...`);
  });

  libp2pHost.addEventListener('peer:disconnect', (evt) => {
    console.error(`peer:disconnect event - peerId=${evt.detail.toString().slice(0, 16)}...`);
  });

  console.error(`Node ${nodeId} started with PeerId: ${peerId.toString()}`);
}

async function connect(nodeIds: number[]): Promise<void> {
  if (!libp2pHost) return;

  for (const targetNodeId of nodeIds) {
    const targetPeerId = await getPeerIdForNode(targetNodeId);
    const targetAddr = multiaddr(`/ip4/11.0.0.${targetNodeId + 1}/tcp/9000/p2p/${targetPeerId.toString()}`);

    try {
      await libp2pHost.dial(targetAddr);
      console.error(`Connected to node ${targetNodeId} (${targetPeerId.toString()})`);
    } catch (e) {
      console.error(`Failed to connect to node ${targetNodeId}: ${e}`);
    }
  }
}

async function waitUntil(elapsedSeconds: number): Promise<void> {
  const targetTime = startTime + elapsedSeconds * 1000;
  let now = Date.now();
  let sleepTime = targetTime - now;
  console.error(`waitUntil: startTime=${startTime} now=${now} targetTime=${targetTime} sleepTime=${sleepTime} elapsedSeconds=${elapsedSeconds}`);

  // Use small sleep intervals to keep the event loop active and send periodic
  // keepalive traffic. Shadow doesn't support TCP keepalive, so we need
  // application-level keepalive to prevent connections from closing after
  // 10 seconds of inactivity.
  const keepaliveInterval = 5000; // Keepalive every 5 seconds

  while (sleepTime > 0) {
    const interval = Math.min(sleepTime, keepaliveInterval);
    await new Promise((resolve) => setTimeout(resolve, interval));
    now = Date.now();
    sleepTime = targetTime - now;

    // Send keepalive traffic to all connected peers by re-dialing them.
    // This sends actual network traffic to prevent idle connection timeout.
    if (libp2pHost && sleepTime > 0) {
      const connections = libp2pHost.getConnections();
      for (const conn of connections) {
        try {
          // Dial the peer to refresh the connection
          // This will reuse the existing connection but may trigger protocol negotiation
          await libp2pHost.dial(conn.remotePeer);
        } catch {
          // Ignore dial errors - connection may already be closed
        }
      }
    }
  }
  console.error(`waitUntil: woke up at ${Date.now()}`);
}

function subscribeToTopic(topicId: string, partial: boolean): void {
  if (!gossipsubRouter) return;

  gossipsubRouter.addEventListener('message', (evt) => {
    handleMessage(evt.detail, topicId);
  });

  if (partial) {
    // Use subscribePartial which handles subscription internally
    // Don't call subscribe() first - it would prevent partial flags from being sent
    gossipsubRouter.subscribePartial(topicId, {
      requestsPartial: true,
      supportsSendingPartial: true,
    });
    console.error(`Subscribed to topic: ${topicId} with partial message support`);
  } else {
    gossipsubRouter.subscribe(topicId);
    console.error(`Subscribed to topic: ${topicId}`);
  }

  subscribedTopics.add(topicId);
}

function handleMessage(message: Message, _topicId: string): void {
  const data = message.data;
  console.error(`Received message on topic ${message.topic}, size=${data.length}`);

  if (data.length >= 8) {
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const messageId = view.getBigInt64(0, false); // big endian

    logJson('Received Message', {
      id: messageId.toString(),
      topic: message.topic,
      from: 'type' in message && message.type === 'signed' ? message.from.toString() : 'unsigned',
    });

    // Check if this is a partial message
    if (data.length > 8) {
      const groupIdStart = data.length - 8;
      const groupIdBytes = data.slice(groupIdStart);
      const groupIdView = new DataView(groupIdBytes.buffer, groupIdBytes.byteOffset, 8);
      const groupId = groupIdView.getBigInt64(0, false);

      let partialMsg = partialMessages.get(groupId);
      if (!partialMsg) {
        partialMsg = new InteropPartialMessage(groupIdBytes);
        partialMessages.set(groupId, partialMsg);
      }

      if (partialMsg.extend(data)) {
        logJson('All parts received', {
          groupId: groupId.toString(),
          topic: message.topic,
        });
      }
    }
  }
}

function publish(messageId: number, messageSizeBytes: number, topicId: string): void {
  if (!gossipsubRouter) return;

  const data = new Uint8Array(messageSizeBytes);
  const view = new DataView(data.buffer);
  view.setBigInt64(0, BigInt(messageId), false); // big endian

  gossipsubRouter.publish(topicId, data);
  console.error(`Published message ${messageId} to topic ${topicId}`);
}

function addPartialMessage(parts: number, topicId: string, groupId: number): void {
  const groupIdBytes = new Uint8Array(8);
  const view = new DataView(groupIdBytes.buffer);
  view.setBigInt64(0, BigInt(groupId), false); // big endian

  let partialMsg = partialMessages.get(BigInt(groupId));
  if (!partialMsg) {
    partialMsg = new InteropPartialMessage(groupIdBytes);
    partialMessages.set(BigInt(groupId), partialMsg);
  }
  partialMsg.fillParts(parts);

  console.error(`Added partial message groupId=${groupId} parts=${parts} to topic ${topicId}`);

  // Check if already complete after adding parts
  if (partialMsg.isComplete()) {
    logJson('All parts received', {
      groupId: groupId.toString(),
      topic: topicId,
    });
  }
}

function publishPartial(topicId: string, groupId: number, _targetNodeIds?: number[] | null): void {
  if (!gossipsubRouter) return;

  const partialMsg = partialMessages.get(BigInt(groupId));
  if (!partialMsg) {
    console.error(`No partial message found for groupId=${groupId}`);
    return;
  }

  // Debug: check mesh and peer states
  const mesh = (gossipsubRouter as unknown as { mesh: Map<string, Set<string>> }).mesh;
  const peerPartialOpts = (gossipsubRouter as unknown as { peerPartialOpts: Map<string, Map<string, { requestsPartial: boolean }>> }).peerPartialOpts;
  const meshPeers = mesh?.get(topicId);
  const topicOpts = peerPartialOpts?.get(topicId);

  console.error(`publishPartial: topicId=${topicId} meshPeers=${meshPeers?.size ?? 0} topicOpts keys=${topicOpts ? Array.from(topicOpts.keys()).length : 0}`);
  if (meshPeers) {
    for (const peerId of meshPeers) {
      const opts = topicOpts?.get(peerId);
      console.error(`  peer ${peerId.slice(0, 16)}... requestsPartial=${opts?.requestsPartial}`);
    }
  }

  gossipsubRouter.publishPartial(partialMsg, topicId);
  console.error(`Published partial message groupId=${groupId} to topic ${topicId}`);
}

function handlePartialRpc(
  fromPeerId: PeerId,
  topicID: Uint8Array,
  groupID: Uint8Array,
  partsMetadata: Uint8Array | undefined,
  partialMessage: Uint8Array | undefined
): void {
  const topicStr = uint8ArrayToString(topicID);
  const groupIdView = new DataView(groupID.buffer, groupID.byteOffset, groupID.byteLength);
  const groupId = groupIdView.getBigInt64(0, false);

  console.error(`handlePartialRpc: from=${fromPeerId.toString().slice(0, 16)}... topic=${topicStr} groupId=${groupId} metaLen=${partsMetadata?.length ?? 0} msgLen=${partialMessage?.length ?? 0}`);

  // Get or create the partial message for this group
  let partialMsg = partialMessages.get(groupId);
  if (!partialMsg) {
    partialMsg = new InteropPartialMessage(groupID);
    partialMessages.set(groupId, partialMsg);
  }

  const beforeParts = partialMsg.partsMetadata()[0];
  const wasComplete = partialMsg.isComplete();

  // Extend with actual received part data (not just metadata)
  // The metadata only tells us what parts the sender has - we need actual data
  if (partialMessage && partialMessage.length > 0) {
    partialMsg.extend(partialMessage);
  }

  // Check if we've now completed the message
  if (!wasComplete && partialMsg.isComplete()) {
    logJson('All parts received', {
      groupId: groupId.toString(),
      topic: topicStr,
    });
  }

  const afterParts = partialMsg.partsMetadata()[0];
  const receivedNewParts = afterParts !== beforeParts;
  console.error(`handlePartialRpc: groupId=${groupId} beforeParts=${beforeParts} afterParts=${afterParts} complete=${partialMsg.isComplete()} newParts=${receivedNewParts}`);

  // Republish whenever we receive new parts to forward them to other mesh peers
  // This is critical for chain topology where parts need to propagate through multiple hops
  if (receivedNewParts && gossipsubRouter) {
    console.error(`handlePartialRpc: republishing because we received new parts (before=${beforeParts} after=${afterParts})`);
    gossipsubRouter.publishPartial(partialMsg, topicStr);
  }
}

async function executeInstruction(instruction: ScriptInstruction): Promise<void> {
  switch (instruction.type) {
    case 'initGossipSub':
      await initGossipSub(instruction.gossipSubParams);
      break;
    case 'connect':
      await connect(instruction.connectTo);
      break;
    case 'waitUntil':
      await waitUntil(instruction.elapsedSeconds);
      break;
    case 'subscribeToTopic':
      subscribeToTopic(instruction.topicID, instruction.partial ?? false);
      break;
    case 'publish':
      publish(instruction.messageID, instruction.messageSizeBytes, instruction.topicID);
      break;
    case 'addPartialMessage':
      addPartialMessage(instruction.parts, instruction.topicID, instruction.groupID);
      break;
    case 'publishPartial':
      publishPartial(instruction.topicID, instruction.groupID, instruction.publishToNodeIDs);
      break;
    case 'ifNodeIDEquals':
      if (instruction.nodeID === nodeId) {
        await executeInstruction(instruction.instruction);
      }
      break;
    case 'setTopicValidationDelay':
      // Not implemented - just ignore
      break;
  }
}

async function main(): Promise<void> {
  // Parse --params argument
  const paramsIndex = process.argv.indexOf('--params');
  if (paramsIndex === -1 || paramsIndex + 1 >= process.argv.length) {
    console.error('Usage: gossipsub-bin --params <params.json>');
    process.exit(1);
  }
  const paramsFile = process.argv[paramsIndex + 1];

  // Get node ID from hostname (format: "node0", "node1", etc.)
  const host = hostname();
  const match = host.match(/node(\d+)/);
  nodeId = match ? parseInt(match[1], 10) : 0;

  // Generate deterministic key from node ID
  privKey = await generateDeterministicKey(nodeId);
  peerId = peerIdFromPrivateKey(privKey);

  // Log PeerID
  logJson('PeerID', { id: peerId.toString(), node_id: nodeId });

  // Parse params
  const paramsText = readFileSync(paramsFile, 'utf-8');
  const params: ExperimentParams = JSON.parse(paramsText);

  // Execute script
  for (const instruction of params.script) {
    await executeInstruction(instruction);
  }

  // Keep running for a bit to handle any remaining messages
  await new Promise((resolve) => setTimeout(resolve, 1000));

  // Clean shutdown
  console.error('Script completed, shutting down');
  if (libp2pHost) {
    await libp2pHost.stop();
  }
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
