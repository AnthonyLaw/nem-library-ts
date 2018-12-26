/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 NEM
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import * as requestPromise from "request-promise-native";
import {Observable, of} from "rxjs";
import {ExtendedNodeExperience} from "../models/node/ExtendedNodeExperience";
import {NisNodeInfo} from "../models/node/NisNodeInfo";
import {Node} from "../models/node/Node";
import {NodeCollection} from "../models/node/NodeCollection";
import {BlockHeight} from "./BlockHttp";
import {NisNodeInfoDTO} from "./debug/NisNodeInfoDTO";
import {HttpEndpoint, ServerConfig} from "./HttpEndpoint";
import {ExtendedNodeExperiencePairDTO} from "./node/ExtendedNodeExperiencePairDTO";
import {NodeCollectionDTO} from "./node/NodeCollectionDTO";
import {NodeDTO} from "./node/NodeDTO";
import {flatMap, map, retryWhen} from "rxjs/operators";

export class NodeHttp extends HttpEndpoint {

  constructor(nodes?: ServerConfig[]) {
    super("node", nodes);
  }

  /**
   * Gets basic information about a node
   * @returns Observable<Node>
   */
  public getNodeInfo(): Observable<Node> {
    return of("info")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((nodeDTO: NodeDTO) => {
          return Node.createFromNodeDTO(nodeDTO);
        })
      )
  }

  /**
   * Gets extended information about a node
   * @returns Observable<NisNodeInfo>
   */
  public getNisNodeInfo(): Observable<NisNodeInfo> {
    return of("extended-info")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((nisNodeInfoDTO: NisNodeInfoDTO) => {
          return NisNodeInfo.createFromNisNodeInfoDTO(nisNodeInfoDTO);
        })
      )
  }

  /**
   * Gets an array of all known nodes in the neighborhood.
   * @returns Observable<NodeCollection>
   */
  public getAllNodes(): Observable<NodeCollection> {
    return of("peer-list/all")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((nodeCollectionDTO: NodeCollectionDTO) => {
          return NodeCollection.createFromNodeCollectionDTO(nodeCollectionDTO);
        })
      )
  }

  /**
   * Gets an array of all nodes with status 'active' in the neighborhood.
   * @returns Observable<Node[]>
   */
  public getActiveNodes(): Observable<Node[]> {
    return of("peer-list/reachable")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((nodeCollectionData) => {
          return nodeCollectionData.data.map((nodeDTO: NodeDTO) => {
            return Node.createFromNodeDTO(nodeDTO);
          });
        })
      )
  }

  /**
   * Gets an array of active nodes in the neighborhood that are selected for broadcasts.
   * @returns Observable<Node[]>
   */
  public getActiveNeighbourNodes(): Observable<Node[]> {
    return of("peer-list/active")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((nodeCollectionData) => {
          return nodeCollectionData.data.map((nodeDTO: NodeDTO) => {
            return Node.createFromNodeDTO(nodeDTO);
          });
        })
      )
  }

  /**
   * Requests the chain height from every node in the active node list and returns the maximum height seen.
   * @returns Observable<BlockHeight>
   */
  public getMaximumChainHeightInActiveNeighborhood(): Observable<BlockHeight> {
    return of("active-peers/max-chain-height")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((blockHeight) => {
          return blockHeight.height;
        })
      )
  }

  /**
   * Requests the chain height from every node in the active node list and returns the maximum height seen.
   * @returns Observable<ExtendedNodeExperience[]>
   */
  public getNodeExperiences(): Observable<ExtendedNodeExperience[]> {
    return of("experiences")
      .pipe(
        flatMap((url) => requestPromise.get(this.nextNode() + url, {json: true})),
        retryWhen(this.replyWhenRequestError),
        map((extendedNodeExperiencePairData) => {
          return extendedNodeExperiencePairData.data.map((extendedNodeExperiencePairDTO: ExtendedNodeExperiencePairDTO) => {
            return ExtendedNodeExperience.createFromExtendedNodeExperiencePairDTO(extendedNodeExperiencePairDTO);
          });
        })
      )
  }

  /**
   * Requests network time.
   * @returns Promise<number>
   */
  public async getNetworkTime(): Promise<number> {
    const url = 'http://23.228.67.85:7890/time-sync/network-time';
    let receiveTimeStamp = 0;
    return requestPromise(url).then(res => {
      res = JSON.parse(res);
      receiveTimeStamp = res.receiveTimeStamp / 1000;
      let nodeTimeStamp = Math.floor(receiveTimeStamp) + Math.floor(new Date().getSeconds() / 10);

      return nodeTimeStamp;
    });
  }
}
